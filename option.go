package tungo

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

// Defaults of a client created with NewClient.
const (
	// DefaultTimeout is the timeout of one attempt of an ordinary call, including reading the body.
	DefaultTimeout = 60 * time.Second
	// DefaultChatTimeout is the timeout of one attempt of a non-streaming chat turn
	// (Chat.Create, Chat.Answer, Client.Ask), which waits for the whole answer.
	DefaultChatTimeout = 600 * time.Second
	// DefaultStreamIdleTimeout is how long a stream may stay silent (no bytes, not even a
	// heart-beat) before it fails with a *TimeoutError. Streams have no total timeout.
	DefaultStreamIdleTimeout = 120 * time.Second
	// DefaultMaxRetries is the number of retries after the first attempt.
	DefaultMaxRetries = 2
)

// Option configures a Client (NewClient, Client.WithOptions) or a single call: every API method
// accepts options as its last arguments, and an option given to a call overrides the client's.
type Option func(*config)

// Middleware wraps the HTTP round trip of every attempt. Call next to continue.
type Middleware func(req *http.Request, next func(*http.Request) (*http.Response, error)) (*http.Response, error)

// config is the immutable configuration of a client, or of one call after its options are applied.
//
// Everything sensitive is kept behind a pointer: fmt prints a nested pointer as an address, so
// no formatting verb applied to a client, or to a value that embeds one, can print a secret.
type config struct {
	apiKey            secret
	baseURL           string
	workspaceID       string
	companyID         string
	webhookSecret     secret
	httpClient        *http.Client
	timeout           time.Duration // 0: DefaultTimeout or DefaultChatTimeout, depending on the call
	streamIdleTimeout time.Duration
	maxRetries        int
	headers           *http.Header // behind a pointer, like the secrets: it may carry credentials
	middlewares       []Middleware
	idempotencyKey    string
	responseInto      **Response
	sleep             func(context.Context, time.Duration) error // test hook; nil: real sleep
}

// secret is a string that fmt cannot print.
type secret struct{ value *string }

func newSecret(s string) secret { return secret{value: &s} }

// get returns the secret, or "" when it is not set.
func (s secret) get() string {
	if s.value == nil {
		return ""
	}
	return *s.value
}

// header returns the default headers; nil when there are none.
func (c *config) header() http.Header {
	if c.headers == nil {
		return nil
	}
	return *c.headers
}

func configFromEnv() config {
	baseURL := os.Getenv("TUNGO_BASE_URL")
	if baseURL == "" {
		baseURL = DefaultBaseURL
	}
	return config{
		apiKey:            newSecret(os.Getenv("TUNGO_API_KEY")),
		baseURL:           baseURL,
		workspaceID:       os.Getenv("TUNGO_WORKSPACE_ID"),
		companyID:         os.Getenv("TUNGO_COMPANY_ID"),
		webhookSecret:     newSecret(os.Getenv("TUNGO_WEBHOOK_SECRET")),
		httpClient:        defaultHTTPClient,
		streamIdleTimeout: DefaultStreamIdleTimeout,
		maxRetries:        DefaultMaxRetries,
	}
}

// with returns a copy of the configuration with the options applied. The receiver is not
// modified and shares no mutable state with the result.
func (c config) with(opts []Option) config {
	headers := c.header().Clone()
	if headers == nil {
		headers = http.Header{}
	}
	c.headers = &headers
	c.middlewares = append([]Middleware(nil), c.middlewares...)
	for _, opt := range opts {
		if opt != nil {
			opt(&c)
		}
	}
	return c
}

// defaultHTTPClient is shared by every client that does not bring its own: connections are
// pooled and kept alive. It has no Timeout on purpose: timeouts are applied per call with
// contexts, so that long-lived streams are not cut.
var defaultHTTPClient = &http.Client{Transport: defaultTransport()}

func defaultTransport() http.RoundTripper {
	t, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return http.DefaultTransport
	}
	t = t.Clone()
	t.MaxIdleConnsPerHost = 16
	return t
}

// WithAPIKey sets the API key (tg_...). The default is the TUNGO_API_KEY environment variable.
// The key is sent only to operations that require it and never appears in errors or debug output.
// An empty key means "not set": the option is ignored and the default stays in force, so
// WithAPIKey(os.Getenv("MY_KEY")) is safe when the variable is missing.
func WithAPIKey(key string) Option {
	return func(c *config) {
		if key != "" {
			c.apiKey = newSecret(key)
		}
	}
}

// WithBaseURL sets the API host, for example a mock server in tests. The default is the
// TUNGO_BASE_URL environment variable, then DefaultBaseURL. A path prefix is kept. An empty
// string means "not set": the option is ignored and the default stays in force, so
// WithBaseURL(os.Getenv("MY_URL")) is safe when the variable is missing.
func WithBaseURL(baseURL string) Option {
	return func(c *config) {
		if baseURL != "" {
			c.baseURL = baseURL
		}
	}
}

// WithCompanyID sets the company (co_...) used by the operations under /v1/companies/{co}.
// The default is the TUNGO_COMPANY_ID environment variable. Pass it to a single call to address
// another company.
func WithCompanyID(id string) Option { return func(c *config) { c.companyID = id } }

// WithWorkspaceID sets the workspace (ws_...) used by the operations under /v1/workspaces/{ws}.
// The default is the TUNGO_WORKSPACE_ID environment variable.
func WithWorkspaceID(id string) Option { return func(c *config) { c.workspaceID = id } }

// WithWebhookSecret sets the signing secret (whsec_...) that Client.VerifyWebhook uses.
// The default is the TUNGO_WEBHOOK_SECRET environment variable.
func WithWebhookSecret(secret string) Option {
	return func(c *config) { c.webhookSecret = newSecret(secret) }
}

// WithHTTPClient sets the HTTP client, for example to use a proxy, custom TLS settings or a
// test transport. The client is never modified. Its transport, cookie jar and Timeout are used
// as they are; its CheckRedirect is not: the SDK never follows a redirect (see APIError for a
// 3xx answer), so that the API key cannot be forwarded to another host. Do not set its Timeout
// field: it would also cut streams; use WithTimeout instead.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *config) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithTimeout sets the timeout of one attempt of a non-streaming call; it covers the whole
// attempt including reading the response body. It replaces both DefaultTimeout and
// DefaultChatTimeout. A deadline on the context of a call is honoured as well: whichever
// comes first wins. Zero or a negative value restores the defaults.
func WithTimeout(d time.Duration) Option {
	return func(c *config) {
		if d < 0 {
			d = 0
		}
		c.timeout = d
	}
}

// WithRequestTimeout is WithTimeout under the name that reads best as a per-request option.
func WithRequestTimeout(d time.Duration) Option { return WithTimeout(d) }

// WithStreamIdleTimeout sets how long a stream may deliver no bytes before it fails with a
// *TimeoutError. The default is DefaultStreamIdleTimeout; the API sends a heart-beat every 15 s.
// Zero or a negative value restores the default.
func WithStreamIdleTimeout(d time.Duration) Option {
	return func(c *config) {
		if d <= 0 {
			d = DefaultStreamIdleTimeout
		}
		c.streamIdleTimeout = d
	}
}

// WithMaxRetries sets how many times a request that is safe to repeat is retried after the first
// attempt. The default is DefaultMaxRetries; 0 disables retries.
func WithMaxRetries(n int) Option {
	return func(c *config) {
		if n < 0 {
			n = 0
		}
		c.maxRetries = n
	}
}

// WithHeader sets a request header, replacing a header of the same name set by an earlier
// option. It cannot override the headers the SDK owns (the API key, Idempotency-Key: use
// WithIdempotencyKey).
func WithHeader(name, value string) Option {
	return func(c *config) {
		if c.headers == nil {
			c.headers = &http.Header{}
		}
		c.headers.Set(name, value)
	}
}

// WithHeaderDel removes a header set by an earlier option, for example a client default.
func WithHeaderDel(name string) Option {
	return func(c *config) { c.header().Del(name) }
}

// WithIdempotencyKey sets the Idempotency-Key of a call instead of the generated one. The same
// key is sent on every attempt. Giving a key to a POST or PATCH operation makes it safe to
// retry. Use it per call, not as a client default.
func WithIdempotencyKey(key string) Option { return func(c *config) { c.idempotencyKey = key } }

// WithResponseInto stores the HTTP response metadata of the call (status, headers, request id,
// rate limit, raw body) in *dst. It is filled for successful calls and for API errors.
func WithResponseInto(dst **Response) Option { return func(c *config) { c.responseInto = dst } }

// WithMiddleware adds a function that wraps the HTTP round trip of every attempt: use it for
// logging, metrics or tracing. Middlewares run in the order they were added. The request
// carries the API key: do not log its headers without Response-style redaction (RedactHeaders).
func WithMiddleware(m Middleware) Option {
	return func(c *config) {
		if m != nil {
			c.middlewares = append(c.middlewares, m)
		}
	}
}

// RedactHeaders returns a copy of the headers that is safe to log: credentials and signatures
// are replaced with "[REDACTED]".
func RedactHeaders(h http.Header) http.Header {
	out := h.Clone()
	for name := range out {
		switch strings.ToLower(name) {
		case "authorization", "x-api-key", "x-signature", "webhook-signature", "svix-signature", "cookie", "set-cookie", "proxy-authorization":
			out[name] = []string{"[REDACTED]"}
		}
	}
	return out
}

// Format implements fmt.Formatter so that no verb (%v, %+v, %#v, %s) prints the API key or the
// webhook secret of a client.
func (c Client) Format(f fmt.State, _ rune) {
	fmt.Fprintf(f, "tungo.Client{BaseURL: %q, APIKey: %s, CompanyID: %q, WorkspaceID: %q}",
		c.cfg.baseURL, redacted(c.cfg.apiKey.get()), c.cfg.companyID, c.cfg.workspaceID)
}

func redacted(secret string) string {
	if secret == "" {
		return "(not set)"
	}
	return "[REDACTED]"
}

// CloseIdleConnections closes the idle keep-alive connections of the client's HTTP client. The
// client stays usable; call it when the program is done talking to the API for a while.
func (c *Client) CloseIdleConnections() { c.cfg.httpClient.CloseIdleConnections() }

// Ptr returns a pointer to v. It is the usual way to fill an optional field:
//
//	tungo.ChatListParams{Limit: tungo.Ptr(int64(20))}
func Ptr[T any](v T) *T { return &v }
