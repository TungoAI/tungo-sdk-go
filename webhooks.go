package tungo

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// DefaultWebhookTolerance is how far the timestamp of a webhook may be from the current time,
// in either direction, before VerifyWebhook rejects it.
const DefaultWebhookTolerance = 300 * time.Second

// WebhookFailure is the reason a webhook was rejected.
type WebhookFailure string

// The reasons VerifyWebhook rejects a delivery.
const (
	// WebhookInvalidTolerance: the tolerance option is zero or negative (it cannot be disabled).
	WebhookInvalidTolerance WebhookFailure = "invalid_tolerance"
	// WebhookInvalidSecret: the secret is empty or is not base64 after the whsec_ prefix.
	WebhookInvalidSecret WebhookFailure = "invalid_secret"
	// WebhookMissingHeader: the id, timestamp or signature header is missing.
	WebhookMissingHeader WebhookFailure = "missing_header"
	// WebhookMalformedTimestamp: the timestamp header is not a whole number of seconds.
	WebhookMalformedTimestamp WebhookFailure = "malformed_timestamp"
	// WebhookTimestampTooOld: the message is older than the tolerance (possibly a replay).
	WebhookTimestampTooOld WebhookFailure = "timestamp_too_old"
	// WebhookTimestampTooNew: the message is dated further in the future than the tolerance.
	WebhookTimestampTooNew WebhookFailure = "timestamp_too_new"
	// WebhookSignatureMismatch: no v1 signature of the header matches the body.
	WebhookSignatureMismatch WebhookFailure = "signature_mismatch"
	// WebhookInvalidPayload: the signature is valid but the body is not a JSON event.
	WebhookInvalidPayload WebhookFailure = "invalid_payload"
)

// WebhookVerificationError reports why a webhook delivery was rejected. Answer such a request
// with 400 and do not process it.
type WebhookVerificationError struct {
	// Reason is the machine-readable cause.
	Reason WebhookFailure
	// Message describes the cause.
	Message string
	// Err is the underlying error, if any.
	Err error
}

func (e *WebhookVerificationError) tungoError() {}

// Error returns the reason and the message.
func (e *WebhookVerificationError) Error() string {
	return fmt.Sprintf("tungo: webhook verification failed (%s): %s", e.Reason, e.Message)
}

// Unwrap returns the underlying error.
func (e *WebhookVerificationError) Unwrap() error { return e.Err }

type webhookConfig struct {
	tolerance time.Duration
	now       func() time.Time
}

// WebhookOption adjusts VerifyWebhook; the defaults (system clock, DefaultWebhookTolerance) are
// right for production.
type WebhookOption func(*webhookConfig)

// WithWebhookTolerance sets how far the timestamp may be from the current time. Zero or a
// negative value does not disable the check: verification then fails with WebhookInvalidTolerance.
func WithWebhookTolerance(d time.Duration) WebhookOption {
	return func(c *webhookConfig) { c.tolerance = d }
}

// WithWebhookClock replaces the system clock; it exists for tests.
func WithWebhookClock(now func() time.Time) WebhookOption {
	return func(c *webhookConfig) {
		if now != nil {
			c.now = now
		}
	}
}

// headerValue finds a header by case-insensitive name, also in a map that was not built with
// http.Header.Set (for example one copied from another framework).
func headerValue(h http.Header, name string) (string, bool) {
	if values, ok := h[http.CanonicalHeaderKey(name)]; ok && len(values) > 0 {
		return values[0], true
	}
	for key, values := range h {
		if strings.EqualFold(key, name) && len(values) > 0 {
			return values[0], true
		}
	}
	return "", false
}

// VerifyWebhook verifies an outgoing webhook delivery (Standard Webhooks / Svix) and returns
// its typed event.
//
// body must be the exact bytes of the request body, before any JSON decoding. header is the
// request header: both the webhook-* and the svix-* names are accepted. secret is the signing
// secret of the endpoint (whsec_...).
//
// The function checks the HMAC-SHA256 signature of "<id>.<timestamp>.<body>" in constant time
// against every v1 signature of the header, and that the timestamp is within
// DefaultWebhookTolerance of the system clock in both directions. Any problem, including an
// empty secret, is a *WebhookVerificationError.
func VerifyWebhook(body []byte, header http.Header, secret string, opts ...WebhookOption) (WebhookEvent, error) {
	cfg := webhookConfig{tolerance: DefaultWebhookTolerance, now: time.Now}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	fail := func(reason WebhookFailure, msg string, err error) (WebhookEvent, error) {
		return WebhookEvent{}, &WebhookVerificationError{Reason: reason, Message: msg, Err: err}
	}
	if cfg.tolerance <= 0 {
		return fail(WebhookInvalidTolerance, "the tolerance must be positive; the timestamp check cannot be disabled", nil)
	}
	encoded := strings.TrimPrefix(secret, "whsec_")
	if encoded == "" {
		return fail(WebhookInvalidSecret, "the webhook secret is empty: pass the endpoint's whsec_... secret, or set WithWebhookSecret / TUNGO_WEBHOOK_SECRET", nil)
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil || len(key) == 0 {
		return fail(WebhookInvalidSecret, "the webhook secret is not valid base64 after the whsec_ prefix", err)
	}

	get := func(suffix string) (string, bool) {
		if v, ok := headerValue(header, "svix-"+suffix); ok {
			return v, true
		}
		return headerValue(header, "webhook-"+suffix)
	}
	id, okID := get("id")
	timestamp, okTS := get("timestamp")
	signatures, okSig := get("signature")
	if !okID || !okTS || !okSig {
		return fail(WebhookMissingHeader, "the webhook-id, webhook-timestamp and webhook-signature headers (or their svix-* forms) are required", nil)
	}
	if timestamp == "" || strings.Trim(timestamp, "0123456789") != "" {
		return fail(WebhookMalformedTimestamp, "the timestamp header is not a whole number of seconds", nil)
	}
	sent, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil {
		return fail(WebhookMalformedTimestamp, "the timestamp header is out of range", err)
	}
	now := cfg.now().Unix()
	tolerance := int64(cfg.tolerance / time.Second)
	if now-sent > tolerance {
		return fail(WebhookTimestampTooOld, "the message is older than the tolerance", nil)
	}
	if sent-now > tolerance {
		return fail(WebhookTimestampTooNew, "the message is dated too far in the future", nil)
	}

	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + timestamp + "."))
	mac.Write(body)
	want := mac.Sum(nil)
	matched := false
	for _, part := range strings.Fields(signatures) {
		version, value, ok := strings.Cut(part, ",")
		if !ok || version != "v1" {
			continue
		}
		got, err := base64.StdEncoding.DecodeString(value)
		if err == nil && hmac.Equal(got, want) {
			matched = true // keep going: the loop must not reveal which signature matched
		}
	}
	if !matched {
		return fail(WebhookSignatureMismatch, "no v1 signature matches the body", nil)
	}

	var event WebhookEvent
	if err := json.Unmarshal(body, &event); err != nil {
		return fail(WebhookInvalidPayload, "the signature is valid but the body is not a JSON event", err)
	}
	if event.Value == nil {
		return fail(WebhookInvalidPayload, "the signature is valid but the body is not a JSON event", nil)
	}
	return event, nil
}

// VerifyWebhook is the package-level VerifyWebhook with the client's webhook secret
// (WithWebhookSecret or TUNGO_WEBHOOK_SECRET).
func (c *Client) VerifyWebhook(body []byte, header http.Header, opts ...WebhookOption) (WebhookEvent, error) {
	return VerifyWebhook(body, header, c.cfg.webhookSecret.get(), opts...)
}

// SignHook computes the X-Signature header of an incoming hook request: "sha256=" followed by
// the lower-case hex HMAC-SHA256 of the exact body bytes. An empty secret is a *ValidationError.
func SignHook(body []byte, secret string) (string, error) {
	if secret == "" {
		return "", &ValidationError{Param: "secret", Message: "the hook secret must not be empty"}
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil)), nil
}

// VerifyHookSignature checks an X-Signature value against the body in constant time. The
// signature may be upper- or lower-case hex, with or without the "sha256=" prefix. It returns
// false for an empty secret, whatever the signature is.
func VerifyHookSignature(body []byte, signature, secret string) bool {
	if secret == "" {
		return false
	}
	signature = strings.TrimSpace(signature)
	if len(signature) >= 7 && strings.EqualFold(signature[:7], "sha256=") {
		signature = signature[7:]
	}
	got, err := hex.DecodeString(signature)
	if err != nil || len(got) != sha256.Size {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hmac.Equal(got, mac.Sum(nil))
}
