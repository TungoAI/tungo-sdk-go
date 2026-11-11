package tungo

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Error is implemented by every error type of this package. Use errors.As with the concrete
// types (*APIError, *ConnectionError, *TimeoutError, *DecodeError, *ConfigError,
// *ValidationError, *PaginationError, *TurnError, *WebhookVerificationError) to tell them
// apart, or with Error itself to recognise any SDK error:
//
//	var sdkErr tungo.Error
//	if errors.As(err, &sdkErr) { ... }
//
// Cancelling the context of a call is not an SDK error: it surfaces as an error for which
// errors.Is(err, context.Canceled) is true.
type Error interface {
	error
	tungoError()
}

// ErrorKind classifies an API error. It is one of the error.type values of the API's error
// envelope; when the response body is not that envelope it is derived from the HTTP status.
type ErrorKind string

// The kinds of API errors.
const (
	// KindInvalidRequest: the request is malformed or failed validation (usually 400, 413, 422).
	KindInvalidRequest ErrorKind = "invalid_request_error"
	// KindAuthentication: the API key is missing, wrong or revoked (401).
	KindAuthentication ErrorKind = "authentication_error"
	// KindPaymentRequired: the balance is exhausted or a payment is needed (402).
	KindPaymentRequired ErrorKind = "payment_required_error"
	// KindPermission: the key lacks a permission (403).
	KindPermission ErrorKind = "permission_error"
	// KindNotFound: the object does not exist (404).
	KindNotFound ErrorKind = "not_found_error"
	// KindConflict: the request conflicts with the current state (409).
	KindConflict ErrorKind = "conflict_error"
	// KindGone: the object existed but is no longer available (410).
	KindGone ErrorKind = "gone_error"
	// KindLimit: a rate limit or a plan limit was hit (429).
	KindLimit ErrorKind = "limit_error"
	// KindAPI: an error on the API side (500).
	KindAPI ErrorKind = "api_error"
	// KindUnavailable: the API or one of its dependencies is temporarily unavailable (502, 503, 504).
	KindUnavailable ErrorKind = "unavailable_error"
)

var knownErrorKinds = map[ErrorKind]bool{
	KindInvalidRequest: true, KindAuthentication: true, KindPaymentRequired: true, KindPermission: true,
	KindNotFound: true, KindConflict: true, KindGone: true, KindLimit: true, KindAPI: true, KindUnavailable: true,
}

// kindForStatus is the fallback classification by HTTP status.
func kindForStatus(status int) ErrorKind {
	switch status {
	case 401:
		return KindAuthentication
	case 402:
		return KindPaymentRequired
	case 403:
		return KindPermission
	case 404:
		return KindNotFound
	case 409:
		return KindConflict
	case 410:
		return KindGone
	case 429:
		return KindLimit
	case 502, 503, 504:
		return KindUnavailable
	}
	if status >= 500 {
		return KindAPI
	}
	return KindInvalidRequest
}

// APIError is an HTTP error response (status 4xx or 5xx) of the API.
type APIError struct {
	// StatusCode is the HTTP status.
	StatusCode int
	// Kind is the class of the error: Type when it is one of the known kinds, otherwise the
	// kind derived from StatusCode. Branch on it.
	Kind ErrorKind
	// Type is error.type exactly as the API sent it (it may be a value newer than this SDK), or
	// the kind derived from the status when the body is not the API's error envelope.
	Type string
	// Code is error.code, a stable machine-readable reason ("" when the API sent none).
	Code string
	// Message is error.message, a text for people. For a body that is not the error envelope it
	// is a short description built by the SDK.
	Message string
	// RequestID identifies the request for support: request_id of the body, else the
	// x-request-id response header.
	RequestID string
	// Fields lists the invalid properties of the request when the API reported them.
	Fields []FieldError
	// Detail is the decoded `error` object of the envelope with every property the contract
	// declares (resets_at, permission, allowed_methods, ...); nil when the body is not the envelope.
	Detail *ErrorDetail
	// Extra holds every property of the `error` object other than type, message, code and fields.
	Extra map[string]json.RawMessage
	// RetryAfter is the delay requested by the retry-after-ms or Retry-After response header;
	// it is positive when HasRetryAfter is true.
	RetryAfter time.Duration
	// HasRetryAfter reports whether the response carried a usable retry-after-ms or Retry-After
	// header. Zero, a negative number and a date in the past are not usable: the SDK then waits
	// its own backoff instead of retrying at once.
	HasRetryAfter bool
	// Header is the response header.
	Header http.Header
	// Body is the raw response body.
	Body []byte
}

func (e *APIError) tungoError() {}

// Error returns a one-line description with the status, the kind, the code and the request id.
func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "tungo: API error %d %s", e.StatusCode, e.Type)
	if e.Code != "" {
		fmt.Fprintf(&b, " (code %s)", e.Code)
	}
	if e.Message != "" {
		b.WriteString(": " + e.Message)
	}
	if e.RequestID != "" {
		fmt.Fprintf(&b, " [request id %s]", e.RequestID)
	}
	return b.String()
}

// Response returns the HTTP metadata of the error response.
func (e *APIError) Response() *Response {
	return &Response{StatusCode: e.StatusCode, Header: e.Header, Body: e.Body}
}

// ConnectionError reports that no HTTP response was received: DNS failure, refused or reset
// connection, TLS failure, or a body that broke off while it was being read.
type ConnectionError struct {
	// Op describes what was being done, for example "GET /v1/workspaces".
	Op string
	// Err is the underlying error.
	Err error
}

func (e *ConnectionError) tungoError() {}

// Error describes the failure; it never contains the API key.
func (e *ConnectionError) Error() string {
	return fmt.Sprintf("tungo: connection error (%s): %v", e.Op, e.Err)
}

// Unwrap returns the underlying error.
func (e *ConnectionError) Unwrap() error { return e.Err }

// TimeoutError reports that a deadline passed: the timeout of a request attempt, the idle
// timeout of a stream, the deadline of the caller's context, or the overall deadline of a wait
// helper. It is a local error, never an HTTP 408.
type TimeoutError struct {
	// Op describes what timed out, for example "GET /v1/workspaces" or "wait for run run_1".
	Op string
	// Limit is the limit that was exceeded; 0 when the deadline came from the caller's context.
	Limit time.Duration
	// Err is the underlying error, usually context.DeadlineExceeded.
	Err error
}

func (e *TimeoutError) tungoError() {}

// Error describes what timed out.
func (e *TimeoutError) Error() string {
	if e.Limit > 0 {
		return fmt.Sprintf("tungo: timeout after %s (%s)", e.Limit, e.Op)
	}
	return fmt.Sprintf("tungo: deadline exceeded (%s)", e.Op)
}

// Unwrap returns the underlying error, so errors.Is(err, context.DeadlineExceeded) works.
func (e *TimeoutError) Unwrap() error { return e.Err }

// Timeout reports true; it makes the error satisfy net.Error-style checks.
func (e *TimeoutError) Timeout() bool { return true }

// DecodeError reports that a successful response (or a stream event) could not be decoded: the
// body is empty, truncated, not JSON, or does not match the contract. It is never retried.
type DecodeError struct {
	// Op describes the call, for example "GET /v1/workspaces".
	Op string
	// StatusCode is the HTTP status of the response.
	StatusCode int
	// RequestID is the x-request-id response header.
	RequestID string
	// Body is the raw body (or the data of the stream event) that failed to decode.
	Body []byte
	// Err is the underlying error, usually from encoding/json.
	Err error
}

func (e *DecodeError) tungoError() {}

// Error describes the failure and includes the request id.
func (e *DecodeError) Error() string {
	msg := fmt.Sprintf("tungo: cannot decode the response of %s (status %d): %v", e.Op, e.StatusCode, e.Err)
	if e.RequestID != "" {
		msg += fmt.Sprintf(" [request id %s]", e.RequestID)
	}
	return msg
}

// Unwrap returns the underlying error.
func (e *DecodeError) Unwrap() error { return e.Err }

// ConfigError reports a problem with the client configuration found before any request was
// sent: no API key for an operation that needs one, no company or workspace id, an invalid base URL.
type ConfigError struct {
	// Message says what is missing or wrong and how to fix it.
	Message string
}

func (e *ConfigError) tungoError() {}

// Error returns the message.
func (e *ConfigError) Error() string { return "tungo: " + e.Message }

// ValidationError reports an invalid argument found before any request was sent: an empty path
// parameter, a body that cannot be encoded, a malformed money amount.
type ValidationError struct {
	// Param names the offending argument.
	Param string
	// Message says what is wrong with it.
	Message string
	// Err is the underlying error, if any.
	Err error
}

func (e *ValidationError) tungoError() {}

// Error names the argument and the problem.
func (e *ValidationError) Error() string {
	return fmt.Sprintf("tungo: invalid %s: %s", e.Param, e.Message)
}

// Unwrap returns the underlying error.
func (e *ValidationError) Unwrap() error { return e.Err }

// PaginationError reports a problem while paging: Next was called on the last page, or the API
// returned the same cursor twice (the auto-paging iterator stops instead of looping forever).
type PaginationError struct {
	// Reason says what went wrong.
	Reason string
	// Cursor is the offending cursor, if any.
	Cursor string
}

func (e *PaginationError) tungoError() {}

// Error returns the reason.
func (e *PaginationError) Error() string {
	if e.Cursor != "" {
		return fmt.Sprintf("tungo: pagination: %s (cursor %q)", e.Reason, e.Cursor)
	}
	return "tungo: pagination: " + e.Reason
}

// TurnError reports that a chat turn failed although the HTTP exchange succeeded: the stream
// carried an `error` event, or the non-streaming result lists errors. It has the same shape as
// an API error but no HTTP status.
type TurnError struct {
	// Kind is the class of the error (Type when it is a known kind, otherwise KindAPI).
	Kind ErrorKind
	// Type is error.type as sent.
	Type string
	// Code is error.code, for example "budget" or "stream_failed".
	Code string
	// Message is the text for people.
	Message string
	// RequestID identifies the request for support.
	RequestID string
	// Extra holds every other property of the error (can_request_more, limit_kind, ...).
	Extra map[string]json.RawMessage
}

func (e *TurnError) tungoError() {}

// Error returns a one-line description with the kind, the code and the request id.
func (e *TurnError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "tungo: chat turn failed: %s", e.Type)
	if e.Code != "" {
		fmt.Fprintf(&b, " (code %s)", e.Code)
	}
	if e.Message != "" {
		b.WriteString(": " + e.Message)
	}
	if e.RequestID != "" {
		fmt.Fprintf(&b, " [request id %s]", e.RequestID)
	}
	return b.String()
}

func newTurnError(detail ChatTurnError, requestID string) *TurnError {
	e := &TurnError{Type: detail.Type, Kind: ErrorKind(detail.Type), Code: string(detail.Code), Message: detail.Message, RequestID: requestID}
	if !knownErrorKinds[e.Kind] {
		e.Kind = KindAPI
	}
	e.Extra = extraFields(json.RawMessage(detail.RawJSON()), "type", "code", "message")
	return e
}

// KindOf returns the kind of an API error or of a failed chat turn found in err's chain, and ""
// for any other error.
func KindOf(err error) ErrorKind {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Kind
	}
	var turnErr *TurnError
	if errors.As(err, &turnErr) {
		return turnErr.Kind
	}
	return ""
}

// maxRetryAfter is the longest server-requested delay the SDK sleeps; a longer one stops the
// retries and the error is returned with RetryAfter filled in.
const maxRetryAfter = 60 * time.Second

var nonNegativeNumber = regexp.MustCompile(`^[0-9]+(\.[0-9]+)?$`)

// parseRetryAfter reads retry-after-ms (milliseconds; it wins) and then Retry-After (seconds,
// possibly fractional, or an HTTP date). The delay is rounded up to a whole millisecond. A value
// that asks for no wait at all (zero, a date that has passed) is reported as absent, so that
// the caller falls back to its backoff: retrying at once would hammer a server in trouble.
func parseRetryAfter(h http.Header, now time.Time) (time.Duration, bool) {
	if ms := strings.TrimSpace(h.Get("retry-after-ms")); nonNegativeNumber.MatchString(ms) {
		if f, err := strconv.ParseFloat(ms, 64); err == nil && f > 0 {
			return ceilDuration(f * float64(time.Millisecond)), true
		}
	}
	value := strings.TrimSpace(h.Get("Retry-After"))
	if value == "" {
		return 0, false
	}
	if nonNegativeNumber.MatchString(value) {
		if f, err := strconv.ParseFloat(value, 64); err == nil && f > 0 {
			return ceilDuration(f * float64(time.Second)), true
		}
		return 0, false
	}
	if at, err := http.ParseTime(value); err == nil {
		if d := at.Sub(now); d > 0 {
			return d, true
		}
	}
	return 0, false
}

// ceilDuration converts nanoseconds to a Duration rounded up to a millisecond, saturating
// instead of overflowing.
func ceilDuration(ns float64) time.Duration {
	const max = float64(1<<63 - 1)
	if ns >= max {
		return time.Duration(1<<63 - 1)
	}
	d := time.Duration(ns)
	if rem := d % time.Millisecond; rem != 0 {
		d += time.Millisecond - rem
	}
	return d
}

// parseAPIError builds the *APIError of an HTTP error response. It never fails: an HTML, empty
// or truncated body yields an error classified by the status.
func parseAPIError(status int, h http.Header, body []byte, now time.Time) *APIError {
	kind := kindForStatus(status)
	e := &APIError{
		StatusCode: status,
		Kind:       kind,
		Type:       string(kind),
		RequestID:  h.Get("x-request-id"),
		Header:     h,
		Body:       body,
	}
	e.RetryAfter, e.HasRetryAfter = parseRetryAfter(h, now)

	var env struct {
		Error     json.RawMessage `json:"error"`
		RequestID *string         `json:"request_id"`
	}
	var props map[string]json.RawMessage
	var head struct {
		Type    *string `json:"type"`
		Message *string `json:"message"`
	}
	isEnvelope := json.Unmarshal(body, &env) == nil && jsonKind(env.Error) == "object" &&
		json.Unmarshal(env.Error, &props) == nil && json.Unmarshal(env.Error, &head) == nil &&
		head.Type != nil && head.Message != nil
	if !isEnvelope {
		e.Message = http.StatusText(status)
		if e.Message == "" {
			e.Message = "HTTP " + strconv.Itoa(status)
		}
		e.Message += " (the response body is not the API error envelope)"
		return e
	}

	e.Type, e.Message = *head.Type, *head.Message
	if knownErrorKinds[ErrorKind(e.Type)] {
		e.Kind = ErrorKind(e.Type)
	}
	if env.RequestID != nil && *env.RequestID != "" {
		e.RequestID = *env.RequestID
	}
	if raw, ok := props["code"]; ok {
		_ = json.Unmarshal(raw, &e.Code)
	}
	if raw, ok := props["fields"]; ok {
		_ = json.Unmarshal(raw, &e.Fields)
	}
	// The typed detail is best effort: a property that changed shape must not hide the error.
	detail := new(ErrorDetail)
	if json.Unmarshal(env.Error, detail) == nil {
		e.Detail = detail
	}
	delete(props, "type")
	delete(props, "message")
	delete(props, "code")
	delete(props, "fields")
	e.Extra = props
	return e
}
