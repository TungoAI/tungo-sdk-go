package tungo

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand/v2"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Response is the HTTP metadata of one call: obtain it with WithResponseInto, from a stream or
// from an *APIError.
type Response struct {
	// StatusCode is the HTTP status.
	StatusCode int
	// Header is the response header.
	Header http.Header
	// Body is the raw response body; nil for a stream.
	Body []byte
	// Attempts is the number of HTTP attempts the call took (1 when nothing was retried).
	Attempts int
}

// RequestID returns the x-request-id response header: quote it when you contact support.
func (r *Response) RequestID() string { return r.Header.Get("x-request-id") }

// RateLimit is the quota state reported by the x-ratelimit-* headers of one response.
type RateLimit struct {
	// Limit is the number of requests allowed in the current window.
	Limit int64
	// Remaining is the number of requests left in the current window.
	Remaining int64
	// Reset is the number of seconds until the window resets.
	Reset int64
}

// RateLimit returns the rate-limit headers of this very response; ok is false when the response
// carried none of them (public endpoints, for example).
func (r *Response) RateLimit() (limit RateLimit, ok bool) {
	parse := func(name string) int64 {
		v := r.Header.Get(name)
		if v == "" {
			return 0
		}
		ok = true
		n, _ := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		return n
	}
	limit.Limit = parse("x-ratelimit-limit")
	limit.Remaining = parse("x-ratelimit-remaining")
	limit.Reset = parse("x-ratelimit-reset")
	return limit, ok
}

type authMode int

const (
	// authNone: public endpoints and incoming hooks; the API key must not be sent.
	authNone authMode = iota
	// authKey: the operation requires the API key.
	authKey
)

// requestSpec describes one API call; the generated resource methods build it.
type requestSpec struct {
	method      string
	path        string      // template, e.g. /v1/companies/{co}/chat/{id}
	pathArgs    []string    // name, value pairs of the explicit path parameters
	query       url.Values  // nil when the call has no query parameters
	header      http.Header // operation headers (X-Signature, X-Event-Id)
	body        any         // JSON body
	rawBody     []byte      // body sent as it is (hooks)
	hasBody     bool
	contentType string // of rawBody; default application/json
	auth        authMode
	idempotent  bool // the contract declares the Idempotency-Key header
	// defaultKey is the Idempotency-Key used when the caller gives none (the event id of a hook
	// event): the backend reads Idempotency-Key before X-Event-Id, so a random key would override it.
	defaultKey  string
	longRunning bool // non-streaming chat turn: DefaultChatTimeout
	stream      bool // server-sent events: no total timeout
}

func (s *requestSpec) setHeader(name string, value *string) {
	if value == nil {
		return
	}
	if s.header == nil {
		s.header = http.Header{}
	}
	s.header.Set(name, *value)
}

// pathSegment percent-encodes a path parameter as one path segment. An empty value, "." and ".."
// are rejected: they would address a different endpoint.
func pathSegment(name, value string) (string, error) {
	switch value {
	case "":
		return "", &ValidationError{Param: name, Message: "the path parameter must not be empty"}
	case ".", "..":
		return "", &ValidationError{Param: name, Message: fmt.Sprintf("the path parameter must not be %q", value)}
	}
	return url.PathEscape(value), nil
}

// buildURL resolves the path template and appends the query string.
func (c *config) buildURL(spec *requestSpec) (string, error) {
	base, err := url.Parse(c.baseURL)
	if err != nil || base.Host == "" || (base.Scheme != "https" && base.Scheme != "http") {
		return "", &ConfigError{Message: fmt.Sprintf("the base URL %q is not an absolute http(s) URL (WithBaseURL / TUNGO_BASE_URL)", c.baseURL)}
	}
	// The path is appended to the raw text: after a query or a fragment it would end up inside
	// them and the call would go to the wrong endpoint. The value is not shown, as a query may
	// hold a token of a gateway.
	if base.RawQuery != "" || base.ForceQuery || base.Fragment != "" || strings.Contains(c.baseURL, "#") {
		return "", &ConfigError{Message: "the base URL must not contain a query string or a fragment (WithBaseURL / TUNGO_BASE_URL)"}
	}
	var b strings.Builder
	b.WriteString(strings.TrimRight(c.baseURL, "/"))
	rest := spec.path
	for {
		open := strings.IndexByte(rest, '{')
		if open < 0 {
			b.WriteString(rest)
			break
		}
		end := strings.IndexByte(rest[open:], '}') + open
		name := rest[open+1 : end]
		b.WriteString(rest[:open])
		rest = rest[end+1:]

		var value string
		found := false
		for i := 0; i+1 < len(spec.pathArgs); i += 2 {
			if spec.pathArgs[i] == name {
				value, found = spec.pathArgs[i+1], true
			}
		}
		if !found {
			switch name {
			case "co":
				if c.companyID == "" {
					return "", &ConfigError{Message: "the company id is not set: use WithCompanyID (on the client or on the call) or TUNGO_COMPANY_ID"}
				}
				value = c.companyID
			case "ws":
				if c.workspaceID == "" {
					return "", &ConfigError{Message: "the workspace id is not set: use WithWorkspaceID (on the client or on the call) or TUNGO_WORKSPACE_ID"}
				}
				value = c.workspaceID
			}
		}
		segment, err := pathSegment(name, value)
		if err != nil {
			return "", err
		}
		b.WriteString(segment)
	}
	if len(spec.query) > 0 {
		b.WriteString("?" + spec.query.Encode())
	}
	return b.String(), nil
}

// queryValue formats a scalar query parameter: booleans as true/false, integers in full,
// strings (dates and date-times are RFC 3339 strings already) as they are.
func queryValue(v any) string {
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.String:
		return rv.String()
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float32, reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 64)
	}
	return fmt.Sprint(v)
}

// setQuery sets a required query parameter.
func setQuery[T any](q url.Values, name string, v T) { q.Set(name, queryValue(v)) }

// addQuery sets an optional query parameter; nil is omitted, an empty string is sent.
func addQuery[T any](q url.Values, name string, v *T) {
	if v != nil {
		addQueryList(q, name, []T{*v})
	}
}

// addQueryList sends a list as repeated keys.
func addQueryList[T any](q url.Values, name string, values []T) {
	for _, v := range values {
		q.Add(name, queryValue(v))
	}
}

// bodyIdempotencyKey returns the idempotency_key field of a JSON object body (approving a
// proposal documents it as the same as the header), trimmed like the API trims it, or "" when
// there is none. The API prefers the header over the field, so a generated header would make
// the caller's own key useless. A key that cannot travel in a header (outside printable ASCII)
// is ignored here.
func bodyIdempotencyKey(payload []byte) string {
	if !bytes.Contains(payload, []byte("idempotency_key")) {
		return ""
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(payload, &object) != nil {
		return ""
	}
	var key string
	if raw, ok := object["idempotency_key"]; !ok || json.Unmarshal(raw, &key) != nil {
		return ""
	}
	key = strings.TrimSpace(key)
	for i := 0; i < len(key); i++ {
		if key[i] < 0x20 || key[i] > 0x7e {
			return ""
		}
	}
	return key
}

// newIdempotencyKey returns a random UUID (version 4) from the system CSPRNG.
func newIdempotencyKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never fails (it aborts the program instead)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}

const (
	backoffBase = 500 * time.Millisecond
	backoffMax  = 8 * time.Second
)

// backoffDelay is the pause before retry number attempt (0-based):
// min(0.5 s * 2^attempt, 8 s) multiplied by a random factor in [0.75, 1.0].
func backoffDelay(attempt int) time.Duration {
	d := float64(backoffMax)
	if attempt < 10 {
		d = math.Min(float64(backoffBase)*math.Pow(2, float64(attempt)), float64(backoffMax))
	}
	return time.Duration(d * (0.75 + 0.25*mathrand.Float64()))
}

// retryableStatus reports whether the HTTP status is worth another attempt.
func retryableStatus(status int) bool {
	switch status {
	case 408, 429, 500, 502, 503, 504:
		return true
	}
	return false
}

// safeToRepeat reports whether the request may be sent again: an idempotent HTTP method, or any
// request that carries an Idempotency-Key.
func safeToRepeat(method string, hasIdempotencyKey bool) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete:
		return true
	}
	return hasIdempotencyKey
}

// sleep waits for d or until the context is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// pause sleeps between attempts and between polls; tests replace it to record the delays.
func (c *config) pause(ctx context.Context, d time.Duration) error {
	if c.sleep != nil {
		return c.sleep(ctx, d)
	}
	return sleep(ctx, d)
}

// errAttemptTimeout and errStreamIdle are the cancellation causes the SDK sets itself; they tell
// its own deadlines apart from the caller's cancellation.
var (
	errAttemptTimeout = errors.New("the request timeout elapsed")
	errStreamIdle     = errors.New("the stream idle timeout elapsed")
)

// call is the result of a successful exchange: a 2xx response. For a stream the body is still
// open and cancel must be called when the stream ends.
type call struct {
	cfg      *config
	op       string
	resp     *http.Response
	body     []byte // buffered body of a non-streaming call
	attempts int
	ctx      context.Context         // stream only: the context the body is read under
	parent   context.Context         // stream only: the caller's context
	cancel   context.CancelCauseFunc // stream only
	idle     *idleTimer              // stream only
}

func (r *call) response() *Response {
	return &Response{StatusCode: r.resp.StatusCode, Header: r.resp.Header, Body: r.body, Attempts: r.attempts}
}

// execute sends the request, retrying when that is safe, and returns the 2xx response.
func (c *Client) execute(ctx context.Context, spec *requestSpec, opts []Option) (*call, error) {
	cfg := c.cfg.with(opts)
	op := spec.method + " " + spec.path
	if ctx == nil {
		return nil, &ValidationError{Param: "ctx", Message: "the context must not be nil"}
	}

	if spec.auth == authKey && cfg.apiKey.get() == "" {
		return nil, &ConfigError{Message: "the API key is not set: use WithAPIKey or the TUNGO_API_KEY environment variable"}
	}
	fullURL, err := cfg.buildURL(spec)
	if err != nil {
		return nil, err
	}
	if err := checkHeaderValues(&cfg, spec); err != nil {
		return nil, err
	}

	var payload []byte
	contentType := ""
	if spec.hasBody {
		if spec.rawBody != nil || spec.body == nil {
			payload = spec.rawBody
			if payload == nil {
				payload = []byte{}
			}
			contentType = spec.contentType
		} else {
			payload, err = json.Marshal(spec.body)
			if err != nil {
				return nil, &ValidationError{Param: "body", Message: "the request body cannot be encoded as JSON: " + err.Error(), Err: err}
			}
		}
		if contentType == "" {
			contentType = "application/json"
		}
	}

	// A key goes only to an operation whose contract declares Idempotency-Key. Anywhere else the
	// API ignores it, so sending it would make a retry look safe when it repeats the action.
	idempotencyKey := ""
	if spec.idempotent {
		idempotencyKey = cfg.idempotencyKey
		if idempotencyKey == "" {
			idempotencyKey = cfg.header().Get("Idempotency-Key")
		}
		if idempotencyKey == "" {
			idempotencyKey = spec.defaultKey
		}
		if idempotencyKey == "" && spec.rawBody == nil {
			idempotencyKey = bodyIdempotencyKey(payload)
		}
		if idempotencyKey == "" {
			idempotencyKey = newIdempotencyKey()
		}
	}
	repeatable := safeToRepeat(spec.method, idempotencyKey != "")

	timeout := cfg.attemptTimeout(spec)

	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, contextError(ctx, op, err)
		}
		var bodyReader io.Reader
		if spec.hasBody {
			bodyReader = bytes.NewReader(payload)
		}
		req, err := http.NewRequestWithContext(ctx, spec.method, fullURL, bodyReader)
		if err != nil {
			return nil, &ConfigError{Message: fmt.Sprintf("cannot build the request %s: %v", op, err)}
		}
		for name, values := range cfg.header() {
			req.Header[name] = append([]string(nil), values...)
		}
		for name, values := range spec.header {
			req.Header[name] = append([]string(nil), values...)
		}
		if spec.stream {
			req.Header.Set("Accept", "text/event-stream")
		} else {
			req.Header.Set("Accept", "application/json")
		}
		req.Header.Set("User-Agent", "tungo-sdk-go/"+Version)
		if spec.hasBody {
			req.Header.Set("Content-Type", contentType)
		}
		// The key goes only to the operations that require it: public endpoints and hooks get none,
		// even if a default header tried to add one.
		req.Header.Del("x-api-key")
		if spec.auth == authKey {
			req.Header.Set("x-api-key", cfg.apiKey.get())
		} else {
			req.Header.Del("Authorization")
		}
		req.Header.Del("Idempotency-Key")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}

		var res *call
		if spec.stream {
			res, err = cfg.attemptStream(ctx, req, op)
		} else {
			res, err = cfg.attemptBuffered(ctx, req, op, timeout)
		}
		if res != nil {
			res.cfg, res.op, res.attempts = &cfg, op, attempt+1
		}
		if err == nil {
			if cfg.responseInto != nil {
				*cfg.responseInto = res.response()
			}
			return res, nil
		}

		// Decide whether to try again.
		var apiErr *APIError
		var wait time.Duration
		retry := repeatable && attempt < cfg.maxRetries
		switch {
		case errors.As(err, &apiErr):
			if cfg.responseInto != nil {
				*cfg.responseInto = &Response{StatusCode: apiErr.StatusCode, Header: apiErr.Header, Body: apiErr.Body, Attempts: attempt + 1}
			}
			retry = retry && retryableStatus(apiErr.StatusCode)
			wait = backoffDelay(attempt)
			if apiErr.HasRetryAfter {
				wait = apiErr.RetryAfter
				if wait > maxRetryAfter {
					retry = false // the server asks for more than we are willing to sleep
				}
			}
		case isConnectionOrTimeout(err):
			wait = backoffDelay(attempt)
		default:
			retry = false
		}
		if !retry || ctx.Err() != nil {
			if ctxErr := ctx.Err(); ctxErr != nil && apiErr == nil {
				return nil, contextError(ctx, op, ctxErr)
			}
			return nil, err
		}
		if sleepErr := cfg.pause(ctx, wait); sleepErr != nil {
			return nil, contextError(ctx, op, sleepErr)
		}
	}
}

func isConnectionOrTimeout(err error) bool {
	var connErr *ConnectionError
	var timeoutErr *TimeoutError
	return errors.As(err, &connErr) || errors.As(err, &timeoutErr)
}

// contextError converts the error of the caller's context: a deadline becomes a *TimeoutError,
// a cancellation stays recognisable with errors.Is(err, context.Canceled).
func contextError(ctx context.Context, op string, err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return &TimeoutError{Op: op, Err: context.DeadlineExceeded}
	}
	cause := context.Cause(ctx)
	if cause != nil && !errors.Is(cause, context.Canceled) {
		return fmt.Errorf("tungo: %s canceled: %w (%w)", op, context.Canceled, cause)
	}
	return fmt.Errorf("tungo: %s canceled: %w", op, context.Canceled)
}

// noRedirect makes the HTTP client hand a 3xx response back instead of following it.
func noRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// roundTrip runs the middlewares around the HTTP client.
//
// Redirects are never followed: net/http would repeat the request, with the X-Api-Key header,
// against whatever host the Location names. A 3xx answer is returned as it is and becomes an
// *APIError. The caller's client is not modified: the policy is set on a copy of the struct,
// which shares the transport, the cookie jar and the connection pool with the original.
func (c *config) roundTrip(req *http.Request) (*http.Response, error) {
	hc := *c.httpClient
	hc.CheckRedirect = noRedirect
	next := hc.Do
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		m, inner := c.middlewares[i], next
		next = func(r *http.Request) (*http.Response, error) { return m(r, inner) }
	}
	return next(req)
}

// validHeaderValue reports whether net/http sends value: no control byte other than a tab.
// net/http refuses any other one only inside Client.Do, where the failure would look like a
// connection error and be retried.
func validHeaderValue(value string) bool {
	for i := 0; i < len(value); i++ {
		if b := value[i]; (b < 0x20 && b != '\t') || b == 0x7f {
			return false
		}
	}
	return true
}

// checkHeaderValues refuses, before any attempt, a header value net/http would not send (for
// example a key read from a file with its trailing newline). The messages name the header,
// never the value.
func checkHeaderValues(cfg *config, spec *requestSpec) error {
	if spec.auth == authKey && !validHeaderValue(cfg.apiKey.get()) {
		return &ConfigError{Message: "the API key contains a control character (for example a trailing newline): check WithAPIKey / TUNGO_API_KEY"}
	}
	if !validHeaderValue(cfg.idempotencyKey) {
		return &ValidationError{Param: "WithIdempotencyKey", Message: "must not contain a control character such as a line break"}
	}
	for name, values := range cfg.header() {
		for _, value := range values {
			if !validHeaderValue(value) {
				return &ConfigError{Message: fmt.Sprintf("the value of the header %q contains a control character such as a line break (WithHeader)", name)}
			}
		}
	}
	for name, values := range spec.header {
		for _, value := range values {
			if !validHeaderValue(value) {
				return &ValidationError{Param: name, Message: "must not contain a control character such as a line break"}
			}
		}
	}
	return nil
}

// transportError classifies a failure of the HTTP exchange. cause is the cancellation cause of
// the attempt's own context (errAttemptTimeout / errStreamIdle) or nil.
func transportError(parent context.Context, op string, err error, cause error, limit time.Duration) error {
	if parentErr := parent.Err(); parentErr != nil {
		return contextError(parent, op, parentErr)
	}
	if errors.Is(cause, errAttemptTimeout) || errors.Is(cause, errStreamIdle) {
		return &TimeoutError{Op: op, Limit: limit, Err: fmt.Errorf("%w: %w", context.DeadlineExceeded, cause)}
	}
	// *url.Error repeats the method and the URL, and the path can hold a hook token: keep only
	// its cause, for timeouts as well, so no error in the chain shows the URL.
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		err = urlErr.Err
	}
	var netErr interface{ Timeout() bool }
	if errors.As(err, &netErr) && netErr.Timeout() {
		return &TimeoutError{Op: op, Err: err}
	}
	return &ConnectionError{Op: op, Err: err}
}

// attemptTimeout is the time one attempt of an ordinary call may take: the configured timeout,
// or the default for the kind of call.
func (c *config) attemptTimeout(spec *requestSpec) time.Duration {
	switch {
	case c.timeout > 0:
		return c.timeout
	case spec.longRunning:
		return DefaultChatTimeout
	default:
		return DefaultTimeout
	}
}

// attemptBuffered performs one attempt of an ordinary call: the timeout covers connecting,
// the response headers and the whole body.
func (c *config) attemptBuffered(parent context.Context, req *http.Request, op string, timeout time.Duration) (*call, error) {
	ctx, cancel := context.WithCancelCause(parent)
	timer := time.AfterFunc(timeout, func() { cancel(errAttemptTimeout) })
	defer func() {
		timer.Stop()
		cancel(nil)
	}()

	resp, err := c.roundTrip(req.WithContext(ctx))
	if err != nil {
		return nil, transportError(parent, op, err, context.Cause(ctx), timeout)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, err := readErrorBody(resp.Body)
		if err != nil && len(body) == 0 {
			return nil, transportError(parent, op, err, context.Cause(ctx), timeout)
		}
		return nil, parseAPIError(resp.StatusCode, resp.Header, body, time.Now())
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, transportError(parent, op, err, context.Cause(ctx), timeout)
	}
	return &call{resp: resp, body: body}, nil
}

// maxErrorBody caps how much of the body of an error response (or of a response that is not
// the expected stream) is read and kept: an error page of any size cannot exhaust the memory.
const maxErrorBody = 1 << 20

// readErrorBody reads at most maxErrorBody bytes of an error response.
func readErrorBody(r io.Reader) ([]byte, error) {
	return io.ReadAll(io.LimitReader(r, maxErrorBody))
}

// isEventStream reports whether a Content-Type header names text/event-stream.
func isEventStream(contentType string) bool {
	mediaType, _, _ := strings.Cut(contentType, ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), "text/event-stream")
}

// isReplayedJSON reports whether the answer to a stream call is a stored result replayed by
// Idempotency-Key: application/json with Idempotent-Replayed: true.
func isReplayedJSON(h http.Header) bool {
	mediaType, _, _ := strings.Cut(h.Get("Content-Type"), ";")
	return strings.EqualFold(strings.TrimSpace(mediaType), "application/json") &&
		strings.EqualFold(strings.TrimSpace(h.Get("Idempotent-Replayed")), "true")
}

// replayAsEventStream turns a replayed ChatTurnResult into the text/event-stream body of the turn
// it stands for: text (the answer), proposal, question, citation, error and choice events, then
// done. The dialog id and the turn number, which a replay carries only in the body, are set as the
// X-Dialog-Id and X-Turn-No headers of h when it lacks them.
func replayAsEventStream(h http.Header, body []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var result map[string]any
	if err := dec.Decode(&result); err != nil || result == nil {
		return nil, errors.New("the replayed response is not a JSON object")
	}
	dialogID, ok := result["dialog_id"].(string)
	if !ok {
		return nil, errors.New("the replayed response is not a chat turn result (no dialog_id)")
	}
	if h.Get("X-Dialog-Id") == "" {
		h.Set("X-Dialog-Id", dialogID)
	}
	if turn, ok := result["turn_no"].(json.Number); ok && h.Get("X-Turn-No") == "" {
		h.Set("X-Turn-No", turn.String())
	}
	var out bytes.Buffer
	emit := func(event map[string]any) {
		data, _ := json.Marshal(event)
		out.WriteString("event: ")
		out.WriteString(event["type"].(string))
		out.WriteString("\ndata: ")
		out.Write(data)
		out.WriteString("\n\n")
	}
	// with copies an entry of the result as an event of the given type, without the drop key.
	with := func(typ string, entry map[string]any, drop string) map[string]any {
		event := make(map[string]any, len(entry)+1)
		for k, v := range entry {
			if k != drop {
				event[k] = v
			}
		}
		event["type"] = typ
		return event
	}
	entries := func(key string) []map[string]any {
		list, _ := result[key].([]any)
		objects := make([]map[string]any, 0, len(list))
		for _, item := range list {
			if object, ok := item.(map[string]any); ok {
				objects = append(objects, object)
			}
		}
		return objects
	}
	answer, _ := result["answer"].(string)
	if answer != "" {
		emit(map[string]any{"type": "text", "delta": answer})
	}
	for _, p := range entries("proposals") {
		event := with("proposal", p, "id")
		event["proposal_id"] = p["id"]
		emit(event)
	}
	for _, q := range entries("questions") {
		emit(with("question", q, ""))
	}
	for _, c := range entries("citations") {
		emit(with("citation", c, ""))
	}
	var requestID any
	if id := h.Get("x-request-id"); id != "" {
		requestID = id
	}
	for _, e := range entries("errors") {
		emit(map[string]any{"type": "error", "error": e, "request_id": requestID})
	}
	done := map[string]any{"type": "done", "answer": answer}
	if reason, ok := result["stop_reason"].(string); ok {
		done["stop_reason"] = reason
	}
	if usage, ok := result["usage"].(map[string]any); ok {
		done["usage"] = with("", usage, "cost_tiyn")
		delete(done["usage"].(map[string]any), "type")
		if cost, ok := usage["cost_tiyn"]; ok {
			done["cost_tiyn"] = cost
		}
	}
	if pending, ok := result["pending_question"].(map[string]any); ok {
		emit(with("choice", pending, ""))
		done["choice_id"] = pending["choice_id"]
	}
	emit(done)
	return out.Bytes(), nil
}

// idleTimer cancels a stream when a read waits for bytes longer than the idle timeout. It runs
// while the response headers are awaited and during every read of the body, and is stopped in
// between (see idleReader).
type idleTimer struct {
	timer   *time.Timer
	timeout time.Duration
}

func (t *idleTimer) reset() { t.timer.Reset(t.timeout) }
func (t *idleTimer) stop()  { t.timer.Stop() }

// attemptStream performs one attempt of a streaming call. There is no total timeout: the idle
// timer bounds the wait for the response headers and then every read of the body.
func (c *config) attemptStream(parent context.Context, req *http.Request, op string) (*call, error) {
	ctx, cancel := context.WithCancelCause(parent)
	idle := &idleTimer{timeout: c.streamIdleTimeout}
	idle.timer = time.AfterFunc(idle.timeout, func() { cancel(errStreamIdle) })

	resp, err := c.roundTrip(req.WithContext(ctx))
	if err != nil {
		idle.stop()
		cause := context.Cause(ctx)
		cancel(nil)
		return nil, transportError(parent, op, err, cause, idle.timeout)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		body, readErr := readErrorBody(resp.Body)
		resp.Body.Close()
		idle.stop()
		cause := context.Cause(ctx)
		cancel(nil)
		if readErr != nil && len(body) == 0 {
			return nil, transportError(parent, op, readErr, cause, idle.timeout)
		}
		return nil, parseAPIError(resp.StatusCode, resp.Header, body, time.Now())
	}
	if contentType := resp.Header.Get("Content-Type"); isReplayedJSON(resp.Header) {
		// A chat turn repeated with the same Idempotency-Key after it finished: the server answers
		// the stored ChatTurnResult instead of a stream. The caller gets the events it stands for.
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			idle.stop()
			cause := context.Cause(ctx)
			cancel(nil)
			return nil, transportError(parent, op, readErr, cause, idle.timeout)
		}
		stream, err := replayAsEventStream(resp.Header, body)
		if err != nil {
			idle.stop()
			cancel(nil)
			return nil, &DecodeError{Op: op, StatusCode: resp.StatusCode, RequestID: resp.Header.Get("x-request-id"), Body: body, Err: err}
		}
		resp.Body = io.NopCloser(bytes.NewReader(stream))
	} else if !isEventStream(contentType) {
		// A success that is not a stream (a proxy page, a JSON answer) must not look like a
		// stream without events.
		body, _ := readErrorBody(resp.Body)
		resp.Body.Close()
		idle.stop()
		cancel(nil)
		return nil, &DecodeError{Op: op, StatusCode: resp.StatusCode, RequestID: resp.Header.Get("x-request-id"), Body: body,
			Err: fmt.Errorf("expected a text/event-stream response, got Content-Type %q", contentType)}
	}
	// From here on the timer runs only while a read of the body waits for bytes.
	idle.stop()
	return &call{resp: resp, ctx: ctx, parent: parent, cancel: cancel, idle: idle}, nil
}

// do performs a non-streaming call and decodes the JSON response into out (nil: no body expected).
func (c *Client) do(ctx context.Context, spec *requestSpec, out any, opts []Option) error {
	res, err := c.execute(ctx, spec, opts)
	if err != nil {
		return err
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(res.body, out); err != nil {
		if len(bytes.TrimSpace(res.body)) == 0 {
			err = errors.New("the response body is empty")
		}
		return &DecodeError{Op: res.op, StatusCode: res.resp.StatusCode, RequestID: res.resp.Header.Get("x-request-id"), Body: res.body, Err: err}
	}
	return nil
}
