package tungo

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "tg_live_SECRETKEY123"

// recorded is one request as the mock server saw it.
type recorded struct {
	Method string
	URI    string
	Header http.Header
	Body   []byte
}

// mock is an in-process API: it records every request and answers with handler.
type mock struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	reqs    []recorded
	handler func(w http.ResponseWriter, r *http.Request, n int)
}

// newMock starts a mock API. handler receives the 1-based number of the request.
func newMock(t *testing.T, handler func(w http.ResponseWriter, r *http.Request, n int)) *mock {
	t.Helper()
	m := &mock{t: t, handler: handler}
	m.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		m.mu.Lock()
		m.reqs = append(m.reqs, recorded{Method: r.Method, URI: r.RequestURI, Header: r.Header.Clone(), Body: body})
		n := len(m.reqs)
		m.mu.Unlock()
		m.handler(w, r, n)
	}))
	t.Cleanup(m.srv.Close)
	return m
}

func (m *mock) requests() []recorded {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recorded(nil), m.reqs...)
}

func (m *mock) last() recorded {
	reqs := m.requests()
	if len(reqs) == 0 {
		m.t.Fatal("the mock received no request")
	}
	return reqs[len(reqs)-1]
}

// client returns a client for the mock that never really sleeps between attempts; the delays it
// would have slept are appended to *sleeps when sleeps is not nil.
func (m *mock) client(sleeps *[]time.Duration, opts ...Option) *Client {
	var mu sync.Mutex
	noSleep := func(c *config) {
		c.sleep = func(ctx context.Context, d time.Duration) error {
			mu.Lock()
			if sleeps != nil {
				*sleeps = append(*sleeps, d)
			}
			mu.Unlock()
			return ctx.Err()
		}
	}
	base := []Option{WithAPIKey(testKey), WithBaseURL(m.srv.URL), WithCompanyID("co_1"), WithWorkspaceID("ws_1"), noSleep}
	return NewClient(append(base, opts...)...)
}

func respond(status int, body string) func(http.ResponseWriter, *http.Request, int) {
	return func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

const errorBody = `{"type":"error","error":{"type":"unavailable_error","message":"try later","code":"busy"},"request_id":"req_1"}`

// ---- configuration and authentication ---------------------------------------------------------

func TestConfigFromEnvironment(t *testing.T) {
	m := newMock(t, respond(200, `{"data":[]}`))
	t.Setenv("TUNGO_API_KEY", "tg_env_key")
	t.Setenv("TUNGO_BASE_URL", m.srv.URL+"/prefix/")
	t.Setenv("TUNGO_COMPANY_ID", "co_env")
	t.Setenv("TUNGO_WORKSPACE_ID", "ws_env")
	client := NewClient()
	if _, err := client.Chat.List(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	got := m.last()
	if got.URI != "/prefix/v1/companies/co_env/chat" {
		t.Errorf("URI %q: the base URL path prefix and the company from the environment must be used", got.URI)
	}
	if got.Header.Get("x-api-key") != "tg_env_key" {
		t.Errorf("x-api-key %q", got.Header.Get("x-api-key"))
	}
	if ua := got.Header.Get("User-Agent"); ua != "tungo-sdk-go/"+Version {
		t.Errorf("User-Agent %q", ua)
	}
	if !regexp.MustCompile(`^\d+\.\d+\.\d+$`).MatchString(Version) {
		t.Errorf("Version %q is not X.Y.Z", Version)
	}

	t.Setenv("TUNGO_BASE_URL", "")
	if c := NewClient(); c.cfg.baseURL != DefaultBaseURL || DefaultBaseURL != "https://api.tungo.kz" {
		t.Errorf("default base URL %q", c.cfg.baseURL)
	}
}

func TestMissingConfigurationFailsBeforeAnyRequest(t *testing.T) {
	m := newMock(t, respond(200, `{}`))
	t.Setenv("TUNGO_API_KEY", "")
	t.Setenv("TUNGO_COMPANY_ID", "")
	t.Setenv("TUNGO_WORKSPACE_ID", "")
	ctx := context.Background()
	var cfgErr *ConfigError

	_, err := NewClient(WithBaseURL(m.srv.URL)).Workspaces.List(ctx)
	if !errors.As(err, &cfgErr) || !strings.Contains(err.Error(), "TUNGO_API_KEY") {
		t.Errorf("missing key: want *ConfigError naming TUNGO_API_KEY, got %T %v", err, err)
	}
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		t.Error("a missing key must not be reported as an API error (fake 401)")
	}
	_, err = NewClient(WithBaseURL(m.srv.URL), WithAPIKey("k")).Chat.List(ctx, nil)
	if !errors.As(err, &cfgErr) || !strings.Contains(err.Error(), "company") {
		t.Errorf("missing company: got %T %v", err, err)
	}
	_, err = NewClient(WithBaseURL(m.srv.URL), WithAPIKey("k")).Platform.GetAccount(ctx)
	if !errors.As(err, &cfgErr) || !strings.Contains(err.Error(), "workspace") {
		t.Errorf("missing workspace: got %T %v", err, err)
	}
	for _, bad := range []string{"api.tungo.kz", "ftp://x", "://"} {
		_, err = NewClient(WithBaseURL(bad), WithAPIKey("k")).Workspaces.List(ctx)
		if !errors.As(err, &cfgErr) {
			t.Errorf("base URL %q: want *ConfigError, got %T %v", bad, err, err)
		}
	}
	if n := len(m.requests()); n != 0 {
		t.Errorf("%d requests were sent for invalid configurations", n)
	}
}

func TestAPIKeyGoesOnlyWhereTheContractRequiresIt(t *testing.T) {
	m := newMock(t, respond(200, `{"data":[],"accepted":true,"event_id":"hev_1","duplicate":false,"run_ids":[]}`))
	// A default header tries to smuggle the key everywhere; the SDK must strip it.
	client := m.client(nil, WithHeader("X-Api-Key", testKey), WithHeader("Authorization", "Bearer "+testKey), WithHeader("X-Trace", "t1"))
	ctx := context.Background()

	if _, err := client.Workspaces.List(ctx); err != nil {
		t.Fatal(err)
	}
	if got := m.last(); got.Header.Get("x-api-key") != testKey || got.Header.Get("X-Trace") != "t1" {
		t.Errorf("authenticated call: headers %v", got.Header)
	}
	if _, err := client.Platform.ListUsageRates(ctx); err != nil {
		t.Fatal(err)
	}
	if got := m.last(); got.Header.Get("x-api-key") != "" || got.Header.Get("Authorization") != "" || got.Header.Get("X-Trace") != "t1" {
		t.Errorf("public endpoint received credentials: %v", got.Header)
	}
	if _, err := client.Automations.PostHook(ctx, "tok", []byte(`{}`), nil); err != nil {
		t.Fatal(err)
	}
	if got := m.last(); got.Header.Get("x-api-key") != "" || got.Header.Get("Authorization") != "" {
		t.Errorf("hook received credentials: %v", got.Header)
	}

	// Public endpoints and hooks work without any key at all.
	t.Setenv("TUNGO_API_KEY", "")
	anon := NewClient(WithBaseURL(m.srv.URL))
	if _, err := anon.Platform.ListUsageRates(ctx); err != nil {
		t.Errorf("public endpoint without a key: %v", err)
	}
	if _, err := anon.SendHook(ctx, "tok", map[string]int{"a": 1}, nil); err != nil {
		t.Errorf("hook without a key: %v", err)
	}
}

func TestSecretsAreNeverPrinted(t *testing.T) {
	client := NewClient(WithAPIKey(testKey), WithWebhookSecret("whsec_c2VjcmV0"), WithBaseURL("http://127.0.0.1:1"), WithMaxRetries(0))
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%d"} {
		for _, v := range []any{client, *client, client.Chat, []any{client}, struct{ C *Client }{client}} {
			out := fmt.Sprintf(format, v)
			if strings.Contains(out, "SECRETKEY") || strings.Contains(out, "c2VjcmV0") {
				t.Errorf("fmt %s of %T prints a secret: %s", format, v, out)
			}
		}
	}
	if out := fmt.Sprintf("%+v", client); !strings.Contains(out, "[REDACTED]") || !strings.Contains(out, "127.0.0.1") {
		t.Errorf("%%+v = %s", out)
	}
	// A connection error must not carry the key either.
	_, err := client.Workspaces.List(context.Background())
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("want *ConnectionError, got %T %v", err, err)
	}
	if strings.Contains(err.Error(), "SECRETKEY") {
		t.Errorf("the error prints the key: %v", err)
	}
	h := RedactHeaders(http.Header{"X-Api-Key": {testKey}, "Authorization": {"Bearer x"}, "X-Signature": {"sha256=aa"}, "Accept": {"a"}})
	if h.Get("X-Api-Key") != "[REDACTED]" || h.Get("Authorization") != "[REDACTED]" || h.Get("X-Signature") != "[REDACTED]" || h.Get("Accept") != "a" {
		t.Errorf("RedactHeaders: %v", h)
	}
}

func TestModelsNeverPrintCredentials(t *testing.T) {
	var secret WebhookSecret
	if err := json.Unmarshal([]byte(`{"endpoint_id":"ep_1","secret":"whsec_TOPSECRET"}`), &secret); err != nil {
		t.Fatal(err)
	}
	hook := AutomationHookCreate{Name: "n", SignatureSecret: Value("TOPSECRET-TOPSECRET")}
	token := "th_TOPSECRET"
	withToken := AutomationHookWithToken{Token: &token, URL: &token}
	for _, format := range []string{"%v", "%+v", "%#v", "%s", "%q"} {
		for _, v := range []any{secret, &secret, []WebhookSecret{secret}, hook, withToken, struct{ S WebhookSecret }{secret}} {
			if out := fmt.Sprintf(format, v); strings.Contains(out, "TOPSECRET") {
				t.Errorf("fmt %s of %T prints a credential: %s", format, v, out)
			}
		}
	}
	if out := fmt.Sprintf("%+v", secret); !strings.Contains(out, "Secret:[REDACTED]") || !strings.Contains(out, "EndpointID:ep_1") {
		t.Errorf("%%+v = %s", out)
	}
	// The value itself, the raw JSON and the encoding are untouched.
	encoded, err := json.Marshal(secret)
	if err != nil || secret.Secret != "whsec_TOPSECRET" || !strings.Contains(secret.RawJSON(), "TOPSECRET") || !strings.Contains(string(encoded), "TOPSECRET") {
		t.Errorf("the credential must stay readable: %q %q %s %v", secret.Secret, secret.RawJSON(), encoded, err)
	}
}

func TestRedirectsAreNeverFollowed(t *testing.T) {
	var stolen atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { stolen.Add(1) }))
	defer other.Close()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL+"/elsewhere", http.StatusFound)
	}))
	defer api.Close()

	own := &http.Client{}
	for _, hc := range []*http.Client{nil, own} {
		opts := []Option{WithAPIKey(testKey), WithBaseURL(api.URL), WithCompanyID("co_1"), WithMaxRetries(0)}
		if hc != nil {
			opts = append(opts, WithHTTPClient(hc))
		}
		client := NewClient(opts...)
		var apiErr *APIError
		if _, err := client.Workspaces.List(context.Background()); !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
			t.Fatalf("an ordinary call must report the 302 as an *APIError, got %T %v", err, err)
		}
		apiErr = nil
		if _, err := client.Chat.CreateStream(context.Background(), ChatMessageRequest{}); !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusFound {
			t.Fatalf("a stream must report the 302 as an *APIError, got %T %v", err, err)
		}
	}
	if own.CheckRedirect != nil {
		t.Error("the caller's HTTP client was modified")
	}
	if n := stolen.Load(); n != 0 {
		t.Errorf("the redirect target received %d request(s)", n)
	}
}

func TestErrorsNeverCarryTheHookToken(t *testing.T) {
	const token = "SECRET_HOOK_TOKEN_123"
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := "http://" + listener.Addr().String()
	listener.Close()
	client := NewClient(WithBaseURL(refused), WithMaxRetries(0))
	_, err = client.SendHook(context.Background(), token, map[string]int{"a": 1}, nil)
	var connErr *ConnectionError
	if !errors.As(err, &connErr) {
		t.Fatalf("want *ConnectionError, got %T %v", err, err)
	}
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		if text := cause.Error() + fmt.Sprintf(" %+v %#v", cause, cause); strings.Contains(text, token) {
			t.Errorf("the error carries the hook token: %s", text)
		}
	}
	if !strings.Contains(err.Error(), "/hooks/{token}") {
		t.Errorf("the error should name the route template: %v", err)
	}
}

func TestClientIsImmutableAndOptionsArePerRequest(t *testing.T) {
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("x-request-id", "req_abc")
		w.Header().Set("x-ratelimit-limit", "60")
		w.Header().Set("x-ratelimit-remaining", strconv.Itoa(59))
		w.Header().Set("x-ratelimit-reset", "30")
		_, _ = io.WriteString(w, `{"data":[],"next_cursor":null}`)
	})
	client := m.client(nil, WithHeader("X-Default", "d"))
	ctx := context.Background()

	var resp *Response
	_, err := client.Chat.List(ctx, nil, WithCompanyID("co_other"), WithHeader("X-Extra", "e"), WithHeaderDel("X-Default"), WithResponseInto(&resp))
	if err != nil {
		t.Fatal(err)
	}
	got := m.last()
	if got.URI != "/v1/companies/co_other/chat" || got.Header.Get("X-Extra") != "e" || got.Header.Get("X-Default") != "" {
		t.Errorf("per-request options not applied: %s %v", got.URI, got.Header)
	}
	if resp == nil || resp.StatusCode != 200 || resp.RequestID() != "req_abc" || resp.Attempts != 1 || !strings.Contains(string(resp.Body), "next_cursor") {
		t.Fatalf("WithResponseInto: %+v", resp)
	}
	if rl, ok := resp.RateLimit(); !ok || rl.Limit != 60 || rl.Remaining != 59 || rl.Reset != 30 {
		t.Errorf("rate limit %+v %v", rl, ok)
	}

	// The next call sees none of it: the client was not modified.
	if _, err := client.Chat.List(ctx, nil); err != nil {
		t.Fatal(err)
	}
	got = m.last()
	if got.URI != "/v1/companies/co_1/chat" || got.Header.Get("X-Extra") != "" || got.Header.Get("X-Default") != "d" {
		t.Errorf("a per-request option leaked into the client: %s %v", got.URI, got.Header)
	}

	derived := client.WithOptions(WithCompanyID("co_2"), WithHeader("X-Default", "d2"))
	if _, err := derived.Chat.List(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got = m.last(); got.URI != "/v1/companies/co_2/chat" || got.Header.Get("X-Default") != "d2" {
		t.Errorf("WithOptions: %s %v", got.URI, got.Header)
	}
	if _, err := client.Chat.List(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got = m.last(); got.URI != "/v1/companies/co_1/chat" || got.Header.Get("X-Default") != "d" {
		t.Errorf("WithOptions modified its receiver: %s %v", got.URI, got.Header)
	}

	// A response without rate-limit headers reports none (no stale shared state).
	m2 := newMock(t, respond(200, `{"data":[]}`))
	var resp2 *Response
	if _, err := m2.client(nil).Workspaces.List(ctx, WithResponseInto(&resp2)); err != nil {
		t.Fatal(err)
	}
	if _, ok := resp2.RateLimit(); ok {
		t.Error("rate limit reported for a response that has no such headers")
	}
}

func TestMiddlewareAndCustomHTTPClient(t *testing.T) {
	m := newMock(t, respond(200, `{"data":[]}`))
	var order []string
	var transportCalls atomic.Int32
	hc := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		transportCalls.Add(1)
		return http.DefaultTransport.RoundTrip(r)
	})}
	mw := func(name string) Middleware {
		return func(req *http.Request, next func(*http.Request) (*http.Response, error)) (*http.Response, error) {
			order = append(order, name)
			return next(req)
		}
	}
	client := m.client(nil, WithHTTPClient(hc), WithMiddleware(mw("a")), WithMiddleware(mw("b")))
	if _, err := client.Workspaces.List(context.Background()); err != nil {
		t.Fatal(err)
	}
	if transportCalls.Load() != 1 || strings.Join(order, "") != "ab" {
		t.Errorf("transport calls %d, middleware order %v", transportCalls.Load(), order)
	}
	if hc.Timeout != 0 {
		t.Error("the SDK must not modify the caller's http.Client")
	}
	client.CloseIdleConnections()
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestConnectionsAreReused(t *testing.T) {
	var conns atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	}
	srv.Start()
	defer srv.Close()
	client := NewClient(WithAPIKey("k"), WithBaseURL(srv.URL))
	for i := 0; i < 20; i++ {
		if _, err := client.Workspaces.List(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := conns.Load(); n != 1 {
		t.Errorf("20 sequential calls opened %d connections, want 1 (keep-alive)", n)
	}
}

func TestConcurrentUse(t *testing.T) {
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("x-ratelimit-remaining", r.URL.Query().Get("limit"))
		_, _ = io.WriteString(w, `{"data":[],"next_cursor":null}`)
	})
	client := m.client(nil)
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var resp *Response
			_, err := client.Chat.List(context.Background(), &ChatListParams{Limit: Ptr(int64(i))}, WithResponseInto(&resp), WithHeader("X-N", strconv.Itoa(i)))
			if err != nil {
				t.Error(err)
				return
			}
			// The rate limit belongs to this very response, not to whichever finished last.
			if rl, _ := resp.RateLimit(); rl.Remaining != int64(i) {
				t.Errorf("call %d got the rate limit of another response: %+v", i, rl)
			}
		}()
	}
	wg.Wait()
}

// ---- request building -------------------------------------------------------------------------

func TestQueryAndPathEncoding(t *testing.T) {
	m := newMock(t, respond(200, `{"data":[],"next_cursor":null,"counts":{},"total":0}`))
	client := m.client(nil, WithCompanyID("co/1 ?#"))
	ctx := context.Background()
	_, err := client.Proposals.List(ctx, &ProposalsListParams{
		Q: Ptr("a+b c&d"), Limit: Ptr(int64(9007199254740993)), DateFrom: Ptr("2026-11-09"), Status: Ptr(""),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := "/v1/companies/co%2F1%20%3F%23/proposals?date_from=2026-11-09&limit=9007199254740993&q=a%2Bb+c%26d&status="
	if got := m.last().URI; got != want {
		t.Errorf("URI\n got %s\nwant %s", got, want)
	}
	_, err = client.Platform.GetUsage(ctx, &PlatformGetUsageParams{From: Ptr("2026-11-09T15:00:00+05:00")})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.last().URI; !strings.HasSuffix(got, "?from=2026-11-09T15%3A00%3A00%2B05%3A00") {
		t.Errorf("date-time query: %s", got)
	}
	// nil params: no query string at all.
	if _, err := client.Proposals.List(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if got := m.last().URI; strings.Contains(got, "?") {
		t.Errorf("unexpected query: %s", got)
	}
	// Required query parameters are passed by value.
	if _, err := client.Forms.Build(ctx, FormsBuildParams{Form: "300.00", Period: "2026-Q2"}); err != nil {
		t.Fatal(err)
	}
	if got := m.last().URI; !strings.HasSuffix(got, "/forms/build?form=300.00&period=2026-Q2") {
		t.Errorf("required query: %s", got)
	}

	before := len(m.requests())
	var verr *ValidationError
	for _, id := range []string{"", ".", ".."} {
		if _, err := client.Chat.Get(ctx, id); !errors.As(err, &verr) || verr.Param != "id" {
			t.Errorf("Chat.Get(%q): want *ValidationError for id, got %v", id, err)
		}
		if err := client.Webhooks.DeleteEndpoint(ctx, id); !errors.As(err, &verr) {
			t.Errorf("DeleteEndpoint(%q): %v", id, err)
		}
		if _, err := client.Chat.Update(ctx, id, ChatDialogPatch{Title: "x"}); !errors.As(err, &verr) {
			t.Errorf("Update(%q): %v", id, err)
		}
	}
	if _, err := client.Chat.List(ctx, nil, WithCompanyID("..")); !errors.As(err, &verr) {
		t.Errorf("company ..: %v", err)
	}
	if len(m.requests()) != before {
		t.Error("a request was sent for an invalid path parameter")
	}
}

func TestRequestBodies(t *testing.T) {
	m := newMock(t, respond(200, `{}`))
	client := m.client(nil)
	ctx := context.Background()
	body := func() string { return string(m.last().Body) }

	// PATCH semantics: absent, explicit null, empty list and zero values are all distinct.
	cases := []struct {
		patch WebhookEndpointPatch
		want  string
	}{
		{WebhookEndpointPatch{}, `{}`},
		{WebhookEndpointPatch{EventTypes: Null[[]string]()}, `{"event_types":null}`},
		{WebhookEndpointPatch{EventTypes: Value([]string{})}, `{"event_types":[]}`},
		{WebhookEndpointPatch{EventTypes: Value([]string(nil))}, `{"event_types":[]}`},
		{WebhookEndpointPatch{EventTypes: Value([]string{"proposal.written"}), Channels: Null[[]string]()}, `{"channels":null,"event_types":["proposal.written"]}`},
		{WebhookEndpointPatch{Enabled: Ptr(false), Description: Ptr("")}, `{"description":"","enabled":false}`},
	}
	for _, c := range cases {
		if _, err := client.Webhooks.UpdateEndpoint(ctx, "ep_1", c.patch); err != nil {
			t.Fatal(err)
		}
		if got := body(); got != c.want {
			t.Errorf("PATCH body %s, want %s", got, c.want)
		}
	}
	if got := m.last(); got.Method != "PATCH" || got.Header.Get("Content-Type") != "application/json" {
		t.Errorf("%s %v", got.Method, got.Header)
	}

	// A required nullable field is always present; required lists and maps are never null.
	if _, err := client.Automations.PutAutonomy(ctx, CompanyAutonomyInput{}); err != nil {
		t.Fatal(err)
	}
	if got := body(); got != `{"levels":{},"max_amount_per_day_tiyn":null,"max_amount_per_document_tiyn":null}` {
		t.Errorf("CompanyAutonomyInput{} = %s", got)
	}
	if _, err := client.Automations.PutAutonomy(ctx, CompanyAutonomyInput{MaxAmountPerDayTiyn: Value(int64(0))}); err != nil {
		t.Fatal(err)
	}
	if got := body(); !strings.Contains(got, `"max_amount_per_day_tiyn":0,`) {
		t.Errorf("explicit zero lost: %s", got)
	}

	// An optional body: nil sends no body at all, a value sends JSON.
	if _, err := client.Proposals.Approve(ctx, "P-1", nil); err != nil {
		t.Fatal(err)
	}
	if got := m.last(); len(got.Body) != 0 || got.Header.Get("Content-Type") != "" {
		t.Errorf("nil optional body sent %q (%s)", got.Body, got.Header.Get("Content-Type"))
	}
	if _, err := client.Proposals.Approve(ctx, "P-1", &ApproveRequest{}); err != nil {
		t.Fatal(err)
	}
	if got := body(); got != `{}` {
		t.Errorf("empty optional body = %s", got)
	}

	// The non-streaming chat call says stream:false, whatever the caller put there.
	if _, err := client.Chat.Create(ctx, ChatMessageRequest{Message: Ptr("привет \x01 \"q\""), Stream: Ptr(true)}); err != nil {
		t.Fatal(err)
	}
	var sent map[string]any
	if err := json.Unmarshal(m.last().Body, &sent); err != nil {
		t.Fatalf("the body is not valid JSON: %v (%s)", err, m.last().Body)
	}
	if sent["stream"] != false || sent["message"] != "привет \x01 \"q\"" {
		t.Errorf("chat body %v", sent)
	}
	if got := m.last().Header.Get("Accept"); got != "application/json" {
		t.Errorf("Accept %q", got)
	}

	// A tagged union in a request: the discriminator is filled in.
	scenario := AutomationScenarioInput{
		Name:    "s",
		Trigger: AutomationScenarioInputTrigger{Value: &AutomationScenarioInputTriggerOption4{}},
	}
	if _, err := client.Automations.CreateScenario(ctx, scenario); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(m.last().Body, &sent); err != nil {
		t.Fatal(err)
	}
	trigger, _ := sent["trigger"].(map[string]any)
	if trigger["kind"] == nil || trigger["kind"] == "" {
		t.Errorf("the discriminator of the union variant was not sent: %s", m.last().Body)
	}

	// A body that is not valid JSON (NaN) is refused locally.
	before := len(m.requests())
	_, err := client.SendHook(ctx, "tok", map[string]float64{"x": math.NaN()}, nil)
	var verr *ValidationError
	if !errors.As(err, &verr) || len(m.requests()) != before {
		t.Errorf("NaN payload: want *ValidationError and no request, got %v", err)
	}
}

// ---- decoding ---------------------------------------------------------------------------------

func TestDecodingIsForwardCompatible(t *testing.T) {
	ctx := context.Background()

	// Unknown fields are kept, unknown enum values survive, int64 is exact.
	m := newMock(t, respond(200, `{"data":[{"id":"ple_1","at":"2026-11-09T10:00:00Z","amount_tiyn":9007199254740993,"balance_after_tiyn":-9223372036854775808,"api_key_id":null,"kind":"brand_new_kind","new_field":{"a":[1,2]}}],"next_cursor":null,"future":true}`))
	page, err := m.client(nil).Platform.GetLedger(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry := page.Data[0]
	if entry.AmountTiyn != 9007199254740993 || entry.BalanceAfterTiyn != math.MinInt64 {
		t.Errorf("int64 not exact: %d %d", entry.AmountTiyn, entry.BalanceAfterTiyn)
	}
	if string(entry.ExtraFields()["new_field"]) != `{"a":[1,2]}` || len(entry.ExtraFields()) != 1 {
		t.Errorf("unknown field lost: %v", entry.ExtraFields())
	}
	if string(page.ExtraFields()["future"]) != "true" || !strings.Contains(page.RawJSON(), `"future":true`) {
		t.Errorf("page extras %v", page.ExtraFields())
	}
	if !strings.Contains(entry.RawJSON(), "brand_new_kind") {
		t.Errorf("unknown enum value lost: %s", entry.RawJSON())
	}
	reencoded, err := json.Marshal(entry)
	if err != nil || !strings.Contains(string(reencoded), `"amount_tiyn":9007199254740993`) {
		t.Errorf("re-encoding: %s %v", reencoded, err)
	}

	// Union fields (the generator audit's HIGH 5): bool | "unknown".
	m = newMock(t, respond(200, `{"year":2026,"forms":[
		{"form":"100.00","title":"t","why":"w","annual":true,"isna":false,"kind":"year","period_kind":"year","coverage":"full","group":"g","periods":[],"required":true},
		{"form":"200.00","title":"t","why":"w","annual":false,"isna":false,"kind":"quarter","period_kind":"quarter","coverage":"full","group":"g","periods":[],"required":"unknown"},
		{"form":"300.00","title":"t","why":"w","annual":false,"isna":false,"kind":"quarter","period_kind":"quarter","coverage":"full","group":"g","periods":[],"required":{"since":2027}}]}`))
	forms, err := m.client(nil).Forms.List(ctx, nil)
	if err != nil {
		t.Fatalf("a valid response with union fields failed to decode: %v", err)
	}
	if r := forms.Forms[0].Required; r.Bool == nil || !*r.Bool || r.String != nil {
		t.Errorf("required:true decoded as %+v", r)
	}
	if r := forms.Forms[1].Required; r.String == nil || *r.String != "unknown" || r.Bool != nil {
		t.Errorf(`required:"unknown" decoded as %+v`, r)
	}
	if r := forms.Forms[2].Required; r.Bool != nil || r.String != nil || r.RawJSON() != `{"since":2027}` {
		t.Errorf("a new shape must be kept raw, got %+v", r)
	}
	if out, _ := json.Marshal(forms.Forms[2].Required); string(out) != `{"since":2027}` {
		t.Errorf("raw union re-encodes as %s", out)
	}

	// string | number | bool | array, and null.
	var conds []AutomationFilterCondition
	raw := `[{"path":"a","op":"eq","value":"x"},{"path":"a","op":"eq","value":1.5},{"path":"a","op":"eq","value":false},
		{"path":"a","op":"in","value":["x",2,true,null]},{"path":"a","op":"exists","value":null},{"path":"a","op":"exists"}]`
	if err := json.Unmarshal([]byte(raw), &conds); err != nil {
		t.Fatalf("AutomationFilterCondition: %v", err)
	}
	v0, _ := conds[0].Value.Get()
	v1, _ := conds[1].Value.Get()
	v2, _ := conds[2].Value.Get()
	v3, _ := conds[3].Value.Get()
	if v0.String == nil || *v0.String != "x" || v1.Number == nil || *v1.Number != 1.5 || v2.Bool == nil || *v2.Bool {
		t.Errorf("scalar variants: %+v %+v %+v", v0, v1, v2)
	}
	if len(v3.Array) != 4 || *v3.Array[0].String != "x" || *v3.Array[1].Number != 2 || !*v3.Array[2].Bool || v3.Array[3].RawJSON() != "null" {
		t.Errorf("array variant: %+v", v3.Array)
	}
	if !conds[4].Value.IsNull() || conds[5].Value.IsNull() || !conds[5].Value.IsZero() {
		t.Errorf("null vs absent: %+v %+v", conds[4].Value, conds[5].Value)
	}
	back, err := json.Marshal(conds)
	if err != nil {
		t.Fatal(err)
	}
	if !sameJSON(t, json.RawMessage(back), json.RawMessage(raw)) {
		t.Errorf("round trip changed the data:\n got %s\nwant %s", back, raw)
	}

	// Objects told apart by their properties (two 2xx responses of one operation).
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n == 1 {
			_, _ = io.WriteString(w, `{"ok":true,"message":"m","requested":2,"approved":2,"excluded":0,"failed":0,"limit":2000,"counts":{"approved":2},"results":[]}`)
			return
		}
		w.WriteHeader(202)
		_, _ = io.WriteString(w, `{"id":"bat_1","kind":"approve_batch","title":"t","started_at":"2026-11-09T10:00:00Z","running":true,"events":0,"async":true,"message":"m","requested":5000}`)
	})
	client := m.client(nil)
	res, err := client.Proposals.ApproveBatch(ctx, BatchApproveRequest{})
	if err != nil || res.BatchApproveResponse == nil || res.BatchApproveJob != nil || res.BatchApproveResponse.Approved != 2 {
		t.Errorf("200: %+v %v", res, err)
	}
	res, err = client.Proposals.ApproveBatch(ctx, BatchApproveRequest{})
	if err != nil || res.BatchApproveJob == nil || res.BatchApproveResponse != nil || res.BatchApproveJob.ID != "bat_1" {
		t.Errorf("202: %+v %v", res, err)
	}
}

func TestTaggedUnions(t *testing.T) {
	var events []ChatEvent
	raw := `[{"type":"text","delta":"hi","step":1},{"type":"tool","name":"n","status":"started"},{"type":"hologram","x":{"y":1}},{"type":"done","stop_reason":"end_turn","later":1},null,{"no_type":true},7]`
	if err := json.Unmarshal([]byte(raw), &events); err != nil {
		t.Fatal(err)
	}
	text, ok := events[0].Value.(*ChatEventText)
	if !ok || text.Delta != "hi" || events[0].Tag() != "text" {
		t.Errorf("text event: %#v", events[0].Value)
	}
	if tool, ok := events[1].Value.(*ChatEventTool); !ok || tool.Status != "started" {
		t.Errorf("tool event: %#v", events[1].Value)
	}
	unknown, ok := events[2].Value.(*UnknownVariant)
	if !ok || unknown.Tag != "hologram" || events[2].Tag() != "hologram" || string(unknown.Raw) != `{"type":"hologram","x":{"y":1}}` {
		t.Errorf("unknown tag: %#v", events[2].Value)
	}
	done, ok := events[3].Value.(*ChatEventDone)
	if !ok || string(done.ExtraFields()["later"]) != "1" {
		t.Errorf("done event: %#v", events[3].Value)
	}
	if !events[4].IsZero() || events[4].Tag() != "" {
		t.Errorf("null must decode into an empty union: %#v", events[4])
	}
	for _, i := range []int{5, 6} {
		if _, ok := events[i].Value.(*UnknownVariant); !ok {
			t.Errorf("event %d must be kept as an unknown variant: %#v", i, events[i].Value)
		}
	}
	back, err := json.Marshal(events[:5])
	if err != nil {
		t.Fatal(err)
	}
	want := `[{"type":"text","delta":"hi","step":1},{"type":"tool","name":"n","status":"started"},{"type":"hologram","x":{"y":1}},{"type":"done","stop_reason":"end_turn"},null]`
	if !sameJSON(t, json.RawMessage(back), json.RawMessage(want)) {
		t.Errorf("round trip:\n got %s\nwant %s", back, want)
	}
	// A variant built in code gets its discriminator.
	out, err := json.Marshal(ChatEvent{Value: &ChatEventText{Delta: "x"}})
	if err != nil || string(out) != `{"delta":"x","type":"text"}` {
		t.Errorf("built variant = %s, %v", out, err)
	}
	// Every webhook event type is a known variant of WebhookEvent.
	names := []WebhookEventName{WebhookEventNameProposalWritten, WebhookEventNamePing}
	for _, name := range names {
		var ev WebhookEvent
		if err := json.Unmarshal([]byte(`{"id":"evt_1","type":"`+string(name)+`","created_at":"2026-11-09T15:00:00.000Z","workspace_id":"ws_1","company_id":"co_1","data":{}}`), &ev); err != nil {
			t.Fatal(err)
		}
		if _, unknown := ev.Value.(*UnknownVariant); unknown || ev.Tag() != string(name) {
			t.Errorf("webhook event %s decoded as %#v", name, ev.Value)
		}
	}
}

func TestInvalidSuccessBodyIsADecodeError(t *testing.T) {
	bodies := map[string]string{
		"html":       "<html><body>welcome</body></html>",
		"truncated":  `{"data":[{"id":"ws_1"`,
		"empty":      "",
		"whitespace": "  \n",
		"wrong type": `{"data":"not a list"}`,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
				w.Header().Set("x-request-id", "req_dec")
				_, _ = io.WriteString(w, body)
			})
			res, err := m.client(nil).Workspaces.List(context.Background())
			var decErr *DecodeError
			if !errors.As(err, &decErr) {
				t.Fatalf("want *DecodeError, got %T %v (result %+v)", err, err, res)
			}
			if res != nil || decErr.StatusCode != 200 || decErr.RequestID != "req_dec" || string(decErr.Body) != body || errors.Unwrap(err) == nil {
				t.Errorf("decode error %+v", decErr)
			}
			if !strings.Contains(err.Error(), "req_dec") || !strings.Contains(err.Error(), "GET /v1/workspaces") {
				t.Errorf("message %q", err)
			}
			if n := len(m.requests()); n != 1 {
				t.Errorf("a decode error was retried: %d requests", n)
			}
		})
	}
	// An empty 204 is fine for an operation without a result.
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(204) })
	if err := m.client(nil).Webhooks.DeleteEndpoint(context.Background(), "ep_1"); err != nil {
		t.Errorf("204: %v", err)
	}
}

// ---- errors -----------------------------------------------------------------------------------

func TestAPIErrorDetails(t *testing.T) {
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"limit_error","message":"Лимит исчерпан","code":"plan_limit_reached","resets_at":"2026-11-09T18:00:00.000Z","brand_new":[1]},"request_id":"req_9"}`)
	})
	var sleeps []time.Duration
	var resp *Response
	_, err := m.client(&sleeps).Workspaces.List(context.Background(), WithResponseInto(&resp))
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("want *APIError, got %T %v", err, err)
	}
	if apiErr.Kind != KindLimit || apiErr.Code != "plan_limit_reached" || apiErr.RequestID != "req_9" || apiErr.StatusCode != 429 {
		t.Errorf("%+v", apiErr)
	}
	if apiErr.Detail == nil || apiErr.Detail.ResetsAt == nil || *apiErr.Detail.ResetsAt != "2026-11-09T18:00:00.000Z" {
		t.Errorf("typed detail: %+v", apiErr.Detail)
	}
	if string(apiErr.Extra["brand_new"]) != "[1]" || string(apiErr.Extra["resets_at"]) == "" {
		t.Errorf("extra: %v", apiErr.Extra)
	}
	// The server asks for an hour: the SDK does not sleep, it returns the error with the delay.
	if !apiErr.HasRetryAfter || apiErr.RetryAfter != time.Hour || len(sleeps) != 0 || len(m.requests()) != 1 {
		t.Errorf("Retry-After 3600: retry after %v, slept %v, %d requests", apiErr.RetryAfter, sleeps, len(m.requests()))
	}
	if resp == nil || resp.StatusCode != 429 || apiErr.Response().StatusCode != 429 {
		t.Errorf("response of an error: %+v", resp)
	}
	want := "tungo: API error 429 limit_error (code plan_limit_reached): Лимит исчерпан [request id req_9]"
	if err.Error() != want {
		t.Errorf("message\n got %s\nwant %s", err, want)
	}
	if KindOf(errors.New("x")) != "" || KindOf(fmt.Errorf("wrapped: %w", err)) != KindLimit {
		t.Error("KindOf")
	}
}

// ---- retries ----------------------------------------------------------------------------------

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestRetriesBackoffAndIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	fail := func(status int) func(http.ResponseWriter, *http.Request, int) {
		return func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.WriteHeader(status)
			_, _ = io.WriteString(w, errorBody)
		}
	}

	// A POST that declares Idempotency-Key: 3 attempts, one random UUIDv4 key for all of them.
	m := newMock(t, fail(503))
	var sleeps []time.Duration
	client := m.client(&sleeps)
	_, err := client.Chat.Create(ctx, ChatMessageRequest{Message: Ptr("q")})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 503 {
		t.Fatalf("got %v", err)
	}
	reqs := m.requests()
	if len(reqs) != 3 {
		t.Fatalf("%d attempts, want 3", len(reqs))
	}
	key := reqs[0].Header.Get("Idempotency-Key")
	if !uuidV4.MatchString(key) || reqs[1].Header.Get("Idempotency-Key") != key || reqs[2].Header.Get("Idempotency-Key") != key {
		t.Errorf("idempotency keys: %q %q %q", key, reqs[1].Header.Get("Idempotency-Key"), reqs[2].Header.Get("Idempotency-Key"))
	}
	if len(sleeps) != 2 || sleeps[0] < 375*time.Millisecond || sleeps[0] > 500*time.Millisecond || sleeps[1] < 750*time.Millisecond || sleeps[1] > time.Second {
		t.Errorf("backoff delays %v, want [0.375-0.5 s, 0.75-1 s]", sleeps)
	}
	// Another call gets another key.
	_, _ = client.Chat.Create(ctx, ChatMessageRequest{Message: Ptr("q")})
	if other := m.last().Header.Get("Idempotency-Key"); other == key || !uuidV4.MatchString(other) {
		t.Errorf("second call key %q (first %q)", other, key)
	}
	// The caller's key is used as it is.
	_, _ = client.Chat.Create(ctx, ChatMessageRequest{Message: Ptr("q")}, WithIdempotencyKey("my-key"), WithMaxRetries(1))
	reqs = m.requests()
	if n := len(reqs); reqs[n-1].Header.Get("Idempotency-Key") != "my-key" || reqs[n-2].Header.Get("Idempotency-Key") != "my-key" || n != 8 {
		t.Errorf("caller key / WithMaxRetries(1): %d requests, key %q", n, reqs[n-1].Header.Get("Idempotency-Key"))
	}

	// A caller's key does not make a POST repeatable when the operation declares no
	// Idempotency-Key: the API ignores it there, and a retry would queue and bill the runs again.
	m = newMock(t, fail(502))
	_, err = m.client(nil).Automations.TestHook(ctx, "hook_1", AutomationHookTestInput{Run: Ptr(true)},
		WithIdempotencyKey("t-1"), WithHeader("Idempotency-Key", "t-2"))
	if n := len(m.requests()); n != 1 || err == nil || m.last().Header.Get("Idempotency-Key") != "" {
		t.Errorf("POST with an undeclared caller key: %d attempts, err %v, key %q", n, err, m.last().Header.Get("Idempotency-Key"))
	}

	// A POST without a key is never retried; a GET is; WithMaxRetries(0) disables retries.
	m = newMock(t, fail(500))
	client = m.client(nil)
	_, _ = client.Chat.Stop(ctx, "d1")
	if n := len(m.requests()); n != 1 || m.last().Header.Get("Idempotency-Key") != "" {
		t.Errorf("POST without a key: %d attempts, key %q", n, m.last().Header.Get("Idempotency-Key"))
	}
	_, _ = client.Workspaces.List(ctx)
	if n := len(m.requests()); n != 1+3 {
		t.Errorf("GET: %d requests in total, want 4", n)
	}
	_, _ = client.Workspaces.List(ctx, WithMaxRetries(0))
	if n := len(m.requests()); n != 1+3+1 {
		t.Errorf("WithMaxRetries(0): %d requests in total, want 5", n)
	}
	_, _ = client.Workspaces.List(ctx, WithMaxRetries(5))
	if n := len(m.requests()); n != 1+3+1+6 {
		t.Errorf("WithMaxRetries(5): %d requests in total, want 11", n)
	}

	// A retry that succeeds, honouring Retry-After and retry-after-ms.
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		switch n {
		case 1:
			w.Header().Set("Retry-After", "1.5")
			w.WriteHeader(429)
		case 2:
			w.Header().Set("retry-after-ms", "250")
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(503)
		default:
			_, _ = io.WriteString(w, `{"data":[]}`)
		}
	})
	sleeps = nil
	var resp *Response
	if _, err := m.client(&sleeps).Workspaces.List(ctx, WithResponseInto(&resp)); err != nil {
		t.Fatal(err)
	}
	if len(sleeps) != 2 || sleeps[0] != 1500*time.Millisecond || sleeps[1] != 250*time.Millisecond || resp.Attempts != 3 {
		t.Errorf("server-directed delays %v, attempts %d", sleeps, resp.Attempts)
	}

	// Statuses that must not be retried.
	for _, status := range []int{400, 401, 403, 404, 409, 422, 501} {
		m = newMock(t, fail(status))
		_, _ = m.client(nil).Workspaces.List(ctx)
		if n := len(m.requests()); n != 1 {
			t.Errorf("status %d was retried (%d attempts)", status, n)
		}
	}
}

func TestBackoffSleepIsCancellable(t *testing.T) {
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(503)
	})
	// A real client: the sleep is real and must end as soon as the context is cancelled.
	client := NewClient(WithAPIKey("k"), WithBaseURL(m.srv.URL))
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := client.Workspaces.List(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("want context.Canceled, got %T %v", err, err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the backoff sleep ignored the cancellation (%v)", elapsed)
	}
	if n := len(m.requests()); n != 1 {
		t.Errorf("%d requests", n)
	}
}

// ---- timeouts and cancellation ----------------------------------------------------------------

func TestTimeoutCoversTheWholeRequest(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		// Headers arrive at once, the body never finishes.
		w.WriteHeader(200)
		_, _ = io.WriteString(w, `{"data":[`)
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	client := m.client(nil, WithMaxRetries(0))
	start := time.Now()
	_, err := client.Workspaces.List(context.Background(), WithRequestTimeout(150*time.Millisecond))
	var timeoutErr *TimeoutError
	if !errors.As(err, &timeoutErr) || timeoutErr.Limit != 150*time.Millisecond || !timeoutErr.Timeout() || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want *TimeoutError (150ms), got %T %v", err, err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("the timeout did not cover the body: took %v", elapsed)
	}

	// The caller's deadline is combined with the SDK timeout: the earlier one wins.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start = time.Now()
	_, err = client.Workspaces.List(ctx, WithTimeout(time.Hour))
	if !errors.As(err, &timeoutErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("caller deadline: got %T %v", err, err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("caller deadline ignored: took %v", elapsed)
	}

	// Cancellation is not a timeout.
	ctx2, cancel2 := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel2)
	_, err = client.Workspaces.List(ctx2)
	if !errors.Is(err, context.Canceled) || errors.As(err, &timeoutErr) {
		t.Errorf("cancellation: got %T %v", err, err)
	}
	// An already cancelled context sends nothing.
	before := len(m.requests())
	_, err = client.Workspaces.List(ctx2)
	if !errors.Is(err, context.Canceled) || len(m.requests()) != before {
		t.Errorf("cancelled context: %v, %d new requests", err, len(m.requests())-before)
	}
}

func TestDefaultTimeouts(t *testing.T) {
	if DefaultTimeout != 60*time.Second || DefaultChatTimeout != 600*time.Second || DefaultStreamIdleTimeout != 120*time.Second || DefaultMaxRetries != 2 {
		t.Fatal("the defaults differ from the specification")
	}
	// A non-streaming chat turn may take ten minutes; everything else one minute. The operation
	// tells which: capture the timeout the SDK reports when the transport never answers.
	hang := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	})}
	cfg := NewClient().cfg
	if got := cfg.attemptTimeout(&requestSpec{}); got != DefaultTimeout {
		t.Errorf("ordinary call: %v", got)
	}
	if got := cfg.attemptTimeout(&requestSpec{longRunning: true}); got != DefaultChatTimeout {
		t.Errorf("chat turn: %v", got)
	}
	client := NewClient(WithAPIKey("k"), WithBaseURL("http://127.0.0.1:9"), WithCompanyID("co_1"), WithHTTPClient(hang), WithMaxRetries(0))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var timeoutErr *TimeoutError
	if _, err := client.Ask(ctx, "q"); !errors.As(err, &timeoutErr) {
		t.Errorf("Ask: %T %v", err, err)
	}
	// An explicit timeout applies to chat turns as well.
	_, err := client.Ask(context.Background(), "q", WithTimeout(40*time.Millisecond))
	if !errors.As(err, &timeoutErr) || timeoutErr.Limit != 40*time.Millisecond {
		t.Errorf("Ask with a timeout: %T %v", err, err)
	}
}

// ---- pagination -------------------------------------------------------------------------------

func proposalsPage(ids []string, next string) string {
	items := make([]string, len(ids))
	for i, id := range ids {
		items[i] = `{"id":"` + id + `"}`
	}
	cursor := "null"
	if next != "" {
		cursor = strconv.Quote(next)
	}
	return `{"data":[` + strings.Join(items, ",") + `],"next_cursor":` + cursor + `,"counts":{},"total":5}`
}

func TestPagination(t *testing.T) {
	ctx := context.Background()
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		switch r.URL.Query().Get("cursor") {
		case "":
			_, _ = io.WriteString(w, proposalsPage([]string{"P-1", "P-2"}, "c2"))
		case "c2":
			_, _ = io.WriteString(w, proposalsPage([]string{"P-3", "P-4"}, "c3"))
		case "c3":
			_, _ = io.WriteString(w, proposalsPage([]string{"P-5"}, ""))
		default:
			w.WriteHeader(400)
		}
	})
	client := m.client(nil)
	params := &ProposalsListParams{Status: Ptr("new"), Limit: Ptr(int64(2))}

	// One page at a time.
	page, err := client.Proposals.List(ctx, params, WithHeader("X-Trace", "t"))
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Data) != 2 || !page.HasNext() || page.Total != 5 {
		t.Fatalf("first page %+v", page)
	}
	page2, err := page.Next(ctx)
	if err != nil || page2.Data[0].ID != "P-3" {
		t.Fatalf("second page %+v %v", page2, err)
	}
	if got := m.last(); got.URI != "/v1/companies/co_1/proposals?cursor=c2&limit=2&status=new" || got.Header.Get("X-Trace") != "t" {
		t.Errorf("the filters and options were not carried to the next page: %s %v", got.URI, got.Header.Get("X-Trace"))
	}
	if params.Cursor != nil {
		t.Error("Next modified the caller's params")
	}
	page3, err := page2.Next(ctx)
	if err != nil || page3.HasNext() {
		t.Fatalf("third page %+v %v", page3, err)
	}
	var pageErr *PaginationError
	if _, err := page3.Next(ctx); !errors.As(err, &pageErr) {
		t.Errorf("Next on the last page: want *PaginationError, got %v", err)
	}

	// The auto-pager is lazy: it fetches a page only when the loop needs it.
	before := len(m.requests())
	var ids []string
	for item, err := range client.Proposals.ListAll(ctx, params) {
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, item.ID)
		if len(ids) == 1 && len(m.requests())-before != 1 {
			t.Errorf("%d requests after the first item, want 1", len(m.requests())-before)
		}
	}
	if strings.Join(ids, ",") != "P-1,P-2,P-3,P-4,P-5" || len(m.requests())-before != 3 {
		t.Errorf("items %v in %d requests", ids, len(m.requests())-before)
	}
	for _, r := range m.requests()[before:] {
		if r.URL().Query().Get("status") != "new" || r.URL().Query().Get("limit") != "2" {
			t.Errorf("filters lost on %s", r.URI)
		}
	}

	// Leaving the loop stops fetching.
	before = len(m.requests())
	for range client.Proposals.ListAll(ctx, params) {
		break
	}
	if n := len(m.requests()) - before; n != 1 {
		t.Errorf("early break fetched %d pages, want 1", n)
	}
	// nil params work.
	n := 0
	for _, err := range client.Proposals.ListAll(ctx, nil) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 5 {
		t.Errorf("%d items with nil params", n)
	}
}

func (r recorded) URL() *urlWrapper { return &urlWrapper{r.URI} }

type urlWrapper struct{ uri string }

func (u *urlWrapper) Query() queryValues {
	_, q, _ := strings.Cut(u.uri, "?")
	out := queryValues{}
	for _, pair := range strings.Split(q, "&") {
		k, v, _ := strings.Cut(pair, "=")
		out[k] = v
	}
	return out
}

type queryValues map[string]string

func (q queryValues) Get(k string) string { return q[k] }

func TestPaginationStopsOnARepeatingCursorAndOnErrors(t *testing.T) {
	ctx := context.Background()
	// The API keeps answering with the same cursor.
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		_, _ = io.WriteString(w, proposalsPage([]string{"P-1"}, "same"))
	})
	var items int
	var last error
	for _, err := range m.client(nil).Proposals.ListAll(ctx, nil) {
		if err != nil {
			last = err
			continue
		}
		items++
	}
	var pageErr *PaginationError
	if !errors.As(last, &pageErr) || pageErr.Cursor != "same" || items != 2 || len(m.requests()) != 2 {
		t.Errorf("repeating cursor: %d items, %d requests, error %v", items, len(m.requests()), last)
	}

	// An empty cursor string ends the iteration like null.
	m = newMock(t, respond(200, `{"data":[{"id":"P-1"}],"next_cursor":"","counts":{},"total":1}`))
	items = 0
	for _, err := range m.client(nil).Proposals.ListAll(ctx, nil) {
		if err != nil {
			t.Fatal(err)
		}
		items++
	}
	if items != 1 || len(m.requests()) != 1 {
		t.Errorf("empty cursor: %d items, %d requests", items, len(m.requests()))
	}

	// An error on a later page is yielded once and ends the iteration.
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n == 1 {
			_, _ = io.WriteString(w, proposalsPage([]string{"P-1"}, "c2"))
			return
		}
		w.WriteHeader(404)
	})
	items, last = 0, nil
	errs := 0
	for _, err := range m.client(nil).Proposals.ListAll(ctx, nil) {
		if err != nil {
			last = err
			errs++
			continue
		}
		items++
	}
	var apiErr *APIError
	if !errors.As(last, &apiErr) || errs != 1 || items != 1 {
		t.Errorf("error page: %d items, %d errors, %v", items, errs, last)
	}
}

// ---- streaming --------------------------------------------------------------------------------

// sseEvent formats one event the way the backend does.
func sseEvent(id int, name, data string) string {
	return fmt.Sprintf("id: %d\nevent: %s\ndata: %s\n\n", id, name, data)
}

func TestStreamingIsIncrementalAndTyped(t *testing.T) {
	step := make(chan struct{})
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Dialog-Id", "dlg_7")
		w.Header().Set("X-Turn-No", "4")
		flusher := w.(http.Flusher)
		events := []string{
			sseEvent(1, "text", `{"type":"text","delta":"Привет","step":1}`),
			"event: ping\ndata: \n\n",
			sseEvent(2, "text", `{"type":"text","delta":", мир","step":1}`),
			sseEvent(3, "done", `{"type":"done","stop_reason":"end_turn","cost_tiyn":12}`),
		}
		for _, ev := range events {
			_, _ = io.WriteString(w, ev)
			flusher.Flush()
			if strings.HasPrefix(ev, "event: ping") {
				continue
			}
			select {
			case <-step: // the next event is written only after the client consumed this one
			case <-r.Context().Done():
				return
			}
		}
	})
	// A total timeout far shorter than the stream must not apply to it.
	client := m.client(nil, WithTimeout(time.Nanosecond))
	stream, err := client.Chat.CreateStream(context.Background(), ChatMessageRequest{Message: Ptr("q"), Stream: Ptr(false)})
	if err != nil {
		t.Fatal(err)
	}
	if turn, ok := stream.TurnNo(); stream.DialogID() != "dlg_7" || !ok || turn != 4 {
		t.Errorf("dialog %q turn %d", stream.DialogID(), turn)
	}
	var tags []string
	for ev, err := range stream.Events() {
		if err != nil {
			t.Fatal(err)
		}
		tags = append(tags, ev.Tag())
		// Had the SDK buffered the body, this event would not be here yet: the server is
		// blocked until we signal it.
		step <- struct{}{}
	}
	if strings.Join(tags, ",") != "text,text,done" {
		t.Errorf("events %v", tags)
	}
	if stream.LastEventID() != "3" {
		t.Errorf("last event id %q", stream.LastEventID())
	}
	req := m.last()
	var body map[string]any
	_ = json.Unmarshal(req.Body, &body)
	if req.Header.Get("Accept") != "text/event-stream" || body["stream"] != true || !uuidV4.MatchString(req.Header.Get("Idempotency-Key")) {
		t.Errorf("stream request: Accept %q, body %v, key %q", req.Header.Get("Accept"), body, req.Header.Get("Idempotency-Key"))
	}
	// A second iteration is refused instead of silently yielding nothing.
	var verr *ValidationError
	for _, err := range stream.Events() {
		if !errors.As(err, &verr) {
			t.Errorf("second iteration: %v", err)
		}
	}
	if err := stream.Close(); err != nil {
		t.Errorf("Close after the end: %v", err)
	}
}

// streamServer serves an endless stream and reports when the server side saw the client leave.
func streamServer(t *testing.T) (*mock, chan struct{}) {
	gone := make(chan struct{}, 8)
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		for i := 1; ; i++ {
			if _, err := io.WriteString(w, sseEvent(i, "text", `{"type":"text","delta":"x"}`)); err != nil {
				break
			}
			flusher.Flush()
			select {
			case <-r.Context().Done():
				gone <- struct{}{}
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
		gone <- struct{}{}
	})
	return m, gone
}

func waitGone(t *testing.T, gone chan struct{}, what string) {
	t.Helper()
	select {
	case <-gone:
	case <-time.After(5 * time.Second):
		t.Errorf("%s: the connection was not closed", what)
	}
}

func TestStreamIsClosedOnBreakCloseAndCancel(t *testing.T) {
	m, gone := streamServer(t)
	client := m.client(nil)
	ctx := context.Background()

	stream, err := client.Chat.CreateStream(ctx, ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, err := range stream.Events() {
		if err != nil {
			t.Fatal(err)
		}
		if n++; n == 3 {
			break
		}
	}
	waitGone(t, gone, "early break")

	// A stream that is never iterated is released by Close.
	stream, err = client.Chat.CreateStream(ctx, ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	_ = stream.Close() // idempotent
	waitGone(t, gone, "Close without iterating")

	// Cancelling the context ends the iteration with context.Canceled.
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err = client.Chat.CreateStream(cctx, ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var last error
	n = 0
	for _, err := range stream.Events() {
		if err != nil {
			last = err
			continue
		}
		if n++; n == 2 {
			cancel()
		}
	}
	if !errors.Is(last, context.Canceled) {
		t.Errorf("cancel: want context.Canceled, got %T %v", last, last)
	}
	waitGone(t, gone, "cancel")

	// Close from another goroutine ends a blocked iteration.
	stream, err = client.Chat.CreateStream(ctx, ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(50*time.Millisecond, func() { _ = stream.Close() })
	for range stream.Events() {
	}
	waitGone(t, gone, "concurrent Close")
}

func TestStreamIdleTimeout(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseEvent(1, "text", `{"type":"text","delta":"x"}`))
		w.(http.Flusher).Flush()
		select { // then silence
		case <-r.Context().Done():
		case <-release:
		}
	})
	stream, err := m.client(nil).Chat.CreateStream(context.Background(), ChatMessageRequest{}, WithStreamIdleTimeout(150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	var got int
	var last error
	start := time.Now()
	for _, err := range stream.Events() {
		if err != nil {
			last = err
			continue
		}
		got++
	}
	var timeoutErr *TimeoutError
	if got != 1 || !errors.As(last, &timeoutErr) || timeoutErr.Limit != 150*time.Millisecond {
		t.Errorf("%d events, error %T %v", got, last, last)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Errorf("idle timeout took %v", elapsed)
	}
}

func TestStreamErrorsAndRetryBeforeFirstByte(t *testing.T) {
	ctx := context.Background()
	// A non-2xx answer to a stream request is the same typed error as for ordinary calls.
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.WriteHeader(402)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"payment_required_error","message":"нет денег","code":"platform_balance_exhausted"},"request_id":"req_s"}`)
	})
	_, err := m.client(nil).Chat.CreateStream(ctx, ChatMessageRequest{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Kind != KindPaymentRequired || apiErr.Code != "platform_balance_exhausted" || apiErr.RequestID != "req_s" {
		t.Errorf("stream error: %T %v", err, err)
	}

	// A stream request is retried before its first byte, with the same Idempotency-Key.
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n < 3 {
			w.WriteHeader(503)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseEvent(1, "done", `{"type":"done"}`))
	})
	stream, err := m.client(nil).Chat.CreateStream(ctx, ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := stream.Accumulate(nil)
	if err != nil || turn.Done == nil || stream.Response().Attempts != 3 {
		t.Errorf("retried stream: %+v %v", turn, err)
	}
	reqs := m.requests()
	if len(reqs) != 3 || reqs[0].Header.Get("Idempotency-Key") != reqs[2].Header.Get("Idempotency-Key") || reqs[0].Header.Get("Idempotency-Key") == "" {
		t.Errorf("%d attempts, keys %q / %q", len(reqs), reqs[0].Header.Get("Idempotency-Key"), reqs[2].Header.Get("Idempotency-Key"))
	}
	// chat.answer declares no Idempotency-Key: its stream is a POST that is not retried.
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) { w.WriteHeader(503) })
	if _, err := m.client(nil).Chat.AnswerStream(ctx, "dlg_1", ChatAnswerRequest{}); err == nil || len(m.requests()) != 1 {
		t.Errorf("AnswerStream: %v, %d attempts", err, len(m.requests()))
	}

	// A connection that breaks mid-stream is a connection error, after the events received so far.
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", "100000")
		_, _ = io.WriteString(w, sseEvent(1, "text", `{"type":"text","delta":"x"}`))
		w.(http.Flusher).Flush()
		conn, _, _ := w.(http.Hijacker).Hijack()
		conn.Close()
	})
	stream, err = m.client(nil).Chat.CreateStream(ctx, ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	turn, err = stream.Accumulate(nil)
	var connErr *ConnectionError
	if !errors.As(err, &connErr) || turn.Text != "x" {
		t.Errorf("broken stream: text %q, error %T %v", turn.Text, err, err)
	}
	if len(m.requests()) != 1 {
		t.Errorf("a stream was retried after its first byte (%d requests)", len(m.requests()))
	}

	// An event whose data is not valid is reported and the stream goes on.
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseEvent(1, "text", `{"type":"text","delta":`)+sseEvent(2, "text", `{"type":"text","delta":"ok"}`))
	})
	stream, err = m.client(nil).Chat.CreateStream(ctx, ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var decodeErrs, good int
	for _, err := range stream.Events() {
		var decErr *DecodeError
		if errors.As(err, &decErr) {
			decodeErrs++
		} else if err == nil {
			good++
		}
	}
	if decodeErrs != 1 || good != 1 {
		t.Errorf("%d decode errors, %d good events", decodeErrs, good)
	}
}

func TestGetEventStreams(t *testing.T) {
	ctx := context.Background()
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Turn-No", "9")
		if strings.Contains(r.URL.Path, "/feed/") {
			_, _ = io.WriteString(w, ": keep-alive\n\nevent: item\ndata: {\"id\":\"f1\",\"cursor\":\"c1\",\"section\":\"done\",\"section_title\":\"s\",\"at\":\"2026-11-09T10:00:00Z\",\"title\":\"t\",\"actions\":[],\"brand_new\":1}\n\nevent: ping\ndata: \n\n")
			return
		}
		// The second event has no "type" in its data: the event name stands in for it.
		_, _ = io.WriteString(w, sseEvent(1, "status", `{"type":"status","text":"Думаю…"}`)+sseEvent(2, "done", `{"stop_reason":"end_turn"}`))
	})
	client := m.client(nil, WithTimeout(time.Millisecond))

	feed, err := client.Feed.ListEvents(ctx, &FeedListEventsParams{Cursor: Ptr("c0"), Once: Ptr("1")})
	if err != nil {
		t.Fatal(err)
	}
	var items []FeedItem
	for item, err := range feed.Events() {
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item)
	}
	if len(items) != 1 || items[0].ID != "f1" || string(items[0].ExtraFields()["brand_new"]) != "1" {
		t.Errorf("feed items %+v", items)
	}
	if got := m.last(); got.Method != "GET" || got.URI != "/v1/companies/co_1/feed/events?cursor=c0&once=1" || got.Header.Get("Accept") != "text/event-stream" || got.Header.Get("Idempotency-Key") != "" {
		t.Errorf("feed stream request: %s %s %v", got.Method, got.URI, got.Header)
	}

	chat, err := client.Chat.ListEvents(ctx, "dlg_1", &ChatListEventsParams{Turn: Ptr(int64(9))})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := chat.Accumulate(nil)
	if err != nil || len(turn.Events) != 2 || turn.TurnNo != 9 || turn.Done == nil || turn.StopReason != "end_turn" {
		t.Errorf("chat events: %+v %v", turn, err)
	}
	if got := m.last(); got.URI != "/v1/companies/co_1/chat/dlg_1/events?turn=9" || got.Header.Get("Accept") != "text/event-stream" {
		t.Errorf("chat stream request: %s", got.URI)
	}

	batch, err := client.Batches.ListEvents(ctx, "bat_1")
	if err != nil {
		t.Fatal(err)
	}
	var raw []SSEEvent
	for ev, err := range batch.RawEvents() {
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, ev)
	}
	if len(raw) != 2 || raw[0].Event != "status" || raw[1].ID != "2" {
		t.Errorf("raw events %+v", raw)
	}
	if got := m.last(); got.URI != "/v1/companies/co_1/batch/bat_1/events" {
		t.Errorf("batch stream request: %s", got.URI)
	}
}

// TestFeedStreamErrorEvent: an `error` event of the feed is the server giving up (the contract
// says reconnect). It must surface as a *StreamError with the code and the message, after the
// items before it and instead of a zero item, and end the iteration.
func TestFeedStreamErrorEvent(t *testing.T) {
	ctx := context.Background()
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("x-request-id", "req_feed")
		_, _ = io.WriteString(w, "event: feed\ndata: {\"id\":\"f1\",\"cursor\":\"c1\",\"section\":\"done\",\"section_title\":\"s\",\"at\":\"2026-11-09T10:00:00Z\",\"title\":\"t\",\"actions\":[]}\n\n"+
			"event: error\ndata: {\"error\":\"feed_failed\",\"message\":\"Лента недоступна\"}\n\n"+
			"event: feed\ndata: {\"id\":\"f2\",\"cursor\":\"c2\",\"section\":\"done\",\"section_title\":\"s\",\"at\":\"2026-11-09T10:00:00Z\",\"title\":\"t\",\"actions\":[]}\n\n")
	})
	client := m.client(nil)
	feed, err := client.Feed.ListEvents(ctx, &FeedListEventsParams{Once: Ptr("1")})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	var failure error
	for item, err := range feed.Events() {
		if err != nil {
			failure = err
			continue
		}
		ids = append(ids, item.ID)
	}
	if strings.Join(ids, ",") != "f1" {
		t.Errorf("items %q, want only f1 before the error", ids)
	}
	var streamErr *StreamError
	if !errors.As(failure, &streamErr) {
		t.Fatalf("error %T %v, want *StreamError", failure, failure)
	}
	if streamErr.Code != "feed_failed" || streamErr.Message != "Лента недоступна" || streamErr.RequestID != "req_feed" {
		t.Errorf("stream error %+v", streamErr)
	}
	var sdkErr Error
	if !errors.As(failure, &sdkErr) || !strings.Contains(failure.Error(), "feed_failed") {
		t.Errorf("not an SDK error or no code in %q", failure)
	}

	// The envelope form keeps its code and extra properties too.
	e := newStreamError([]byte(`{"error":{"type":"api_error","code":"feed_failed","message":"m","retry":true},"request_id":"req_2"}`), "req_h")
	if e.Code != "feed_failed" || e.Message != "m" || e.RequestID != "req_2" || string(e.Extra["retry"]) != "true" || string(e.Extra["type"]) != `"api_error"` {
		t.Errorf("envelope stream error %+v", e)
	}
}

func TestSSEVeryLongLine(t *testing.T) {
	// A single data line of 9 MB must not fail ("token too long") nor be truncated.
	long := strings.Repeat("я", 9<<20/2)
	stream := "event: text\ndata: " + long + "\n\ndata: tail\n\n"
	var got []SSEEvent
	for ev, err := range ParseSSE(strings.NewReader(stream)) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, ev)
	}
	if len(got) != 2 || got[0].Data != long || got[1].Data != "tail" {
		t.Errorf("%d events, first data length %d (want %d)", len(got), len(got[0].Data), len(long))
	}
	// A read error is reported after the events that were complete.
	broken := io.MultiReader(strings.NewReader("data: a\n\ndata: b"), errReader{})
	var n int
	var last error
	for _, err := range ParseSSE(broken) {
		if err != nil {
			last = err
			continue
		}
		n++
	}
	if n != 1 || last == nil {
		t.Errorf("%d events, error %v", n, last)
	}
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset") }

// ---- chat helpers -----------------------------------------------------------------------------

func TestAsk(t *testing.T) {
	ctx := context.Background()
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("X-Dialog-Id", "dlg_1")
		_, _ = io.WriteString(w, `{"dialog_id":"dlg_1","turn_no":1,"answer":"Сальдо: 45 000,00 ₸","proposals":[],"pending_question":null,"questions":[],"citations":[],"errors":[]}`)
	})
	client := m.client(nil)
	res, err := client.Ask(ctx, "Какое сальдо?")
	if err != nil {
		t.Fatal(err)
	}
	if res.Answer != "Сальдо: 45 000,00 ₸" || res.DialogID != "dlg_1" {
		t.Errorf("%+v", res)
	}
	got := m.last()
	if got.Method != "POST" || got.URI != "/v1/companies/co_1/chat" || string(got.Body) != `{"message":"Какое сальдо?","stream":false}` || got.Header.Get("Idempotency-Key") == "" {
		t.Errorf("%s %s %s", got.Method, got.URI, got.Body)
	}
	// Continuing the dialog uses the typed request.
	if _, err := client.Chat.Create(ctx, ChatMessageRequest{Message: Ptr("ещё"), DialogID: Value(res.DialogID)}); err != nil {
		t.Fatal(err)
	}
	if got := string(m.last().Body); got != `{"dialog_id":"dlg_1","message":"ещё","stream":false}` {
		t.Errorf("follow-up body %s", got)
	}

	// A turn that failed is an error, with the result still available.
	m = newMock(t, respond(200, `{"dialog_id":"dlg_1","turn_no":2,"answer":"","proposals":[],"pending_question":null,"questions":[],"citations":[],"errors":[{"type":"limit_error","code":"budget","message":"Дневной лимит исчерпан.","limit_kind":"day"}]}`))
	res, err = m.client(nil).Ask(ctx, "q")
	var turnErr *TurnError
	if !errors.As(err, &turnErr) || turnErr.Kind != KindLimit || turnErr.Code != "budget" || string(turnErr.Extra["limit_kind"]) != `"day"` || res == nil {
		t.Errorf("failed turn: %T %v (%+v)", err, err, res)
	}
	if KindOf(err) != KindLimit {
		t.Error("KindOf(turn error)")
	}

	// A nil result is never dereferenced by callers: an HTTP error returns (nil, err).
	m = newMock(t, respond(404, `{"type":"error","error":{"type":"not_found_error","message":"нет"},"request_id":null}`))
	res, err = m.client(nil).Ask(ctx, "q")
	if res != nil || KindOf(err) != KindNotFound {
		t.Errorf("%+v %v", res, err)
	}
}

// ---- webhooks and hooks -----------------------------------------------------------------------

// signWebhook produces Standard Webhooks headers for the body.
func signWebhook(secret, id string, at time.Time, body []byte) http.Header {
	key, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	ts := strconv.FormatInt(at.Unix(), 10)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + ts + "."))
	mac.Write(body)
	h := http.Header{}
	h.Set("webhook-id", id)
	h.Set("webhook-timestamp", ts)
	h.Set("webhook-signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	return h
}

func TestVerifyWebhookUsesTheSystemClockAndTheClientSecret(t *testing.T) {
	const secret = "whsec_MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="
	body := []byte(`{"id":"evt_1","type":"proposal.written","created_at":"2026-11-09T15:00:00.000Z","workspace_id":"ws_1","company_id":"co_1","data":{"proposal_id":"P-000042","kind":"Платёжное поручение"}}`)

	// Signed now: valid against the real clock, with no clock passed.
	event, err := VerifyWebhook(body, signWebhook(secret, "msg_1", time.Now(), body), secret)
	if err != nil {
		t.Fatal(err)
	}
	written, ok := event.Value.(*WebhookEventProposalWritten)
	if !ok || written.CompanyID == nil || *written.CompanyID != "co_1" {
		t.Fatalf("typed event: %#v", event.Value)
	}
	// Replay protection is on by default, in both directions.
	var verr *WebhookVerificationError
	_, err = VerifyWebhook(body, signWebhook(secret, "msg_1", time.Now().Add(-10*time.Minute), body), secret)
	if !errors.As(err, &verr) || verr.Reason != WebhookTimestampTooOld {
		t.Errorf("stale message: %v", err)
	}
	_, err = VerifyWebhook(body, signWebhook(secret, "msg_1", time.Now().Add(10*time.Minute), body), secret)
	if !errors.As(err, &verr) || verr.Reason != WebhookTimestampTooNew {
		t.Errorf("message from the future: %v", err)
	}

	// An empty secret is rejected even when the signature was made with an empty key.
	forged := signWebhook("whsec_", "msg_1", time.Now(), body)
	for _, empty := range []string{"", "whsec_"} {
		if _, err := VerifyWebhook(body, forged, empty); !errors.As(err, &verr) || verr.Reason != WebhookInvalidSecret {
			t.Errorf("empty secret %q accepted a forged signature: %v", empty, err)
		}
	}

	// The client verifies with its own secret: from the option or from the environment.
	client := NewClient(WithWebhookSecret(secret))
	if _, err := client.VerifyWebhook(body, signWebhook(secret, "msg_2", time.Now(), body)); err != nil {
		t.Errorf("client secret from the option: %v", err)
	}
	t.Setenv("TUNGO_WEBHOOK_SECRET", secret)
	if _, err := NewClient().VerifyWebhook(body, signWebhook(secret, "msg_3", time.Now(), body)); err != nil {
		t.Errorf("client secret from TUNGO_WEBHOOK_SECRET: %v", err)
	}
	t.Setenv("TUNGO_WEBHOOK_SECRET", "")
	if _, err := NewClient().VerifyWebhook(body, forged); !errors.As(err, &verr) || verr.Reason != WebhookInvalidSecret {
		t.Errorf("client without a secret: %v", err)
	}

	// A valid signature over something that is not an event is still an error.
	garbage := []byte(`not json`)
	if _, err := VerifyWebhook(garbage, signWebhook(secret, "msg_4", time.Now(), garbage), secret); !errors.As(err, &verr) || verr.Reason != WebhookInvalidPayload {
		t.Errorf("garbage payload: %v", err)
	}
	var sdkErr Error
	if !errors.As(err, &sdkErr) {
		t.Error("*WebhookVerificationError must be a tungo.Error")
	}
}

func TestSendHookAndPostHookBehaveIdentically(t *testing.T) {
	ctx := context.Background()
	accepted := `{"accepted":true,"event_id":"hev_1","duplicate":false,"run_ids":["run_1"]}`
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n%3 != 0 {
			w.WriteHeader(503) // two failures, then success: both paths must retry
			return
		}
		w.WriteHeader(202)
		_, _ = io.WriteString(w, accepted)
	})
	client := m.client(nil)
	const token, secret = "tok/../x?y=1", "hksec_test"
	payload := map[string]any{"id": "order_1", "note": "Оплата"}
	raw, _ := json.Marshal(payload)
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(raw)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	res, err := client.SendHook(ctx, token, payload, &SendHookParams{Secret: secret, EventID: "evt-77"})
	if err != nil || !res.Accepted || res.RunIDs[0] != "run_1" {
		t.Fatalf("SendHook: %+v %v", res, err)
	}
	viaHelper := m.requests()
	res, err = client.Automations.PostHook(ctx, token, raw, &AutomationsPostHookParams{Signature: &signature, EventID: Ptr("evt-77")})
	if err != nil || !res.Accepted {
		t.Fatalf("PostHook: %+v %v", res, err)
	}
	viaResource := m.requests()[len(viaHelper):]

	for name, reqs := range map[string][]recorded{"SendHook": viaHelper, "PostHook": viaResource} {
		if len(reqs) != 3 {
			t.Fatalf("%s: %d attempts, want 3", name, len(reqs))
		}
		for _, r := range reqs {
			if r.Method != "POST" || r.URI != "/hooks/tok%2F..%2Fx%3Fy=1" {
				t.Errorf("%s: %s %s (the token must be one escaped path segment)", name, r.Method, r.URI)
			}
			if string(r.Body) != string(raw) || r.Header.Get("X-Signature") != signature || r.Header.Get("X-Event-Id") != "evt-77" {
				t.Errorf("%s: body %s, signature %q, event id %q", name, r.Body, r.Header.Get("X-Signature"), r.Header.Get("X-Event-Id"))
			}
			if !VerifyHookSignature(r.Body, r.Header.Get("X-Signature"), secret) {
				t.Errorf("%s: the signature does not verify against the bytes that were sent", name)
			}
			if r.Header.Get("x-api-key") != "" || r.Header.Get("Content-Type") != "application/json" {
				t.Errorf("%s: headers %v", name, r.Header)
			}
			// The backend reads Idempotency-Key before X-Event-Id: the event id must be the key.
			if key := r.Header.Get("Idempotency-Key"); key != "evt-77" {
				t.Errorf("%s: Idempotency-Key %q, want the event id", name, key)
			}
		}
	}

	// A key the caller gives wins over the event id; without an event id the key is random.
	m = newMock(t, respond(202, accepted))
	client = m.client(nil)
	if _, err := client.SendHook(ctx, "tok", payload, &SendHookParams{EventID: "evt-77"}, WithIdempotencyKey("own")); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendHook(ctx, "tok", payload, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendHook(ctx, "tok", payload, nil); err != nil {
		t.Fatal(err)
	}
	keys := m.requests()
	if keys[0].Header.Get("Idempotency-Key") != "own" || keys[0].Header.Get("X-Event-Id") != "evt-77" {
		t.Errorf("explicit key: %v", keys[0].Header)
	}
	if a, b := keys[1].Header.Get("Idempotency-Key"), keys[2].Header.Get("Idempotency-Key"); a == "" || a == b {
		t.Errorf("generated keys %q and %q", a, b)
	}

	// Arbitrary bytes and content types go out untouched; without a secret there is no signature.
	binary := []byte{0x00, 0xff, 0x10, '\n'}
	m = newMock(t, respond(200, accepted))
	client = m.client(nil)
	if _, err := client.SendHook(ctx, "tok", binary, &SendHookParams{ContentType: "application/octet-stream"}); err != nil {
		t.Fatal(err)
	}
	if got := m.last(); string(got.Body) != string(binary) || got.Header.Get("Content-Type") != "application/octet-stream" || got.Header.Get("X-Signature") != "" {
		t.Errorf("binary hook: %v %v", got.Body, got.Header)
	}
	if _, err := client.SendHook(ctx, "tok", `<xml/>`, &SendHookParams{ContentType: "application/xml", Secret: "s"}); err != nil {
		t.Fatal(err)
	}
	if got := m.last(); string(got.Body) != `<xml/>` || !VerifyHookSignature(got.Body, got.Header.Get("X-Signature"), "s") {
		t.Errorf("string hook: %s", got.Body)
	}
	var verr *ValidationError
	if _, err := client.SendHook(ctx, "", payload, nil); !errors.As(err, &verr) {
		t.Errorf("empty token: %v", err)
	}
}

// ---- long-running helpers ---------------------------------------------------------------------

func runJSON(state string) string {
	return `{"id":"run_1","scenario_id":"scn_1","state":` + state + `,"trigger":"manual","attempt":1,"auto_retries":0,"cancel_requested":false,"created_at":"2026-11-09T10:00:00Z","retryable":false,"trial":false,"steps":[],"tools":[],"proposals":[],"output":{},"answer":null,"current_step":null,"error":null,"error_code":null,"event_id":null,"event_type":null,"external_id":null,"finished_at":null,"output_error":null,"output_note":null,"requested_by":null,"retry_at":null,"slot_at":null,"started_at":null,"stop_reason":null,"event":null}`
}

func TestWaitForRun(t *testing.T) {
	ctx := context.Background()
	states := []string{`"queued"`, `"running"`, `"from_the_future"`, `"running"`, `"done"`}
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, n int) {
		if r.Method == "POST" {
			w.WriteHeader(202)
			_, _ = io.WriteString(w, runJSON(`"queued"`))
			return
		}
		_, _ = io.WriteString(w, runJSON(states[min(n-2, len(states)-1)]))
	})
	var sleeps []time.Duration
	client := m.client(&sleeps)
	run, err := client.RunScenarioAndWait(ctx, "scn_1", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	reqs := m.requests()
	if run.State != AutomationRunDetailStateDone || len(reqs) != 6 {
		t.Errorf("state %q after %d requests", run.State, len(reqs))
	}
	if reqs[0].Method != "POST" || reqs[0].URI != "/v1/companies/co_1/automations/scenarios/scn_1/run" || len(reqs[0].Body) != 0 || reqs[0].Header.Get("Idempotency-Key") == "" {
		t.Errorf("start request: %+v", reqs[0])
	}
	if reqs[1].URI != "/v1/companies/co_1/automations/runs/run_1" || reqs[1].Header.Get("Idempotency-Key") != "" {
		t.Errorf("poll request: %s %v", reqs[1].URI, reqs[1].Header)
	}
	// The pause grows from 1 s to at most 5 s.
	want := []time.Duration{time.Second, 1500 * time.Millisecond, 2250 * time.Millisecond, 3375 * time.Millisecond}
	if fmt.Sprint(sleeps) != fmt.Sprint(want) {
		t.Errorf("poll pauses %v, want %v", sleeps, want)
	}
	if _, interval, maxInterval := (&WaitOptions{PollInterval: time.Minute}).resolved(); interval != 5*time.Second || maxInterval != 5*time.Second {
		t.Errorf("the pause must be capped: %v %v", interval, maxInterval)
	}

	// Every terminal state of the contract ends the wait; others do not.
	for state, terminal := range map[AutomationRunState]bool{"done": true, "error": true, "skipped": true, "cancelled": true, "queued": false, "running": false, "": false, "paused": false} {
		if runFinished(state) != terminal {
			t.Errorf("runFinished(%q) = %v", state, !terminal)
		}
	}

	// A run that never finishes ends with a local *TimeoutError, not with an HTTP error.
	m = newMock(t, respond(200, runJSON(`"running"`)))
	_, err = NewClient(WithAPIKey("k"), WithBaseURL(m.srv.URL), WithCompanyID("co_1")).WaitForRun(ctx, "run_1", &WaitOptions{Timeout: 120 * time.Millisecond, PollInterval: 10 * time.Millisecond})
	var timeoutErr *TimeoutError
	var apiErr *APIError
	if !errors.As(err, &timeoutErr) || errors.As(err, &apiErr) || timeoutErr.Limit != 120*time.Millisecond || len(m.requests()) < 2 {
		t.Errorf("deadline: %T %v after %d polls", err, err, len(m.requests()))
	}
	// Cancellation wins over the deadline.
	cctx, cancel := context.WithCancel(ctx)
	time.AfterFunc(30*time.Millisecond, cancel)
	_, err = NewClient(WithAPIKey("k"), WithBaseURL(m.srv.URL), WithCompanyID("co_1")).WaitForRun(cctx, "run_1", &WaitOptions{PollInterval: 5 * time.Millisecond})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("cancel: %T %v", err, err)
	}
	// A run without a state is not "finished".
	m = newMock(t, respond(200, `{"id":"run_1"}`))
	var decErr *DecodeError
	if _, err = m.client(nil).WaitForRun(ctx, "run_1", nil); !errors.As(err, &decErr) {
		t.Errorf("missing state: %T %v", err, err)
	}
	// An API error ends the wait at once.
	m = newMock(t, respond(404, `{"type":"error","error":{"type":"not_found_error","message":"нет"},"request_id":null}`))
	if _, err = m.client(nil).WaitForRun(ctx, "run_1", nil); KindOf(err) != KindNotFound || len(m.requests()) != 1 {
		t.Errorf("404: %v after %d requests", err, len(m.requests()))
	}
}

func TestWaitForBatch(t *testing.T) {
	ctx := context.Background()
	job := func(running string) string {
		return `{"id":"bat_1","kind":"bank_statement","title":"t","started_at":"2026-11-09T10:00:00Z","events":3` + running + `}`
	}
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, n int) {
		switch {
		case r.Method == "POST":
			w.WriteHeader(202)
			_, _ = io.WriteString(w, job(`,"running":true`))
		case n < 4:
			_, _ = io.WriteString(w, job(`,"running":true`))
		default:
			_, _ = io.WriteString(w, job(`,"running":false,"finished_at":"2026-11-09T10:05:00Z"`))
		}
	})
	client := m.client(nil)
	started, err := client.UploadBankStatement(ctx, "statement.txt", []byte("1CClientBankExchange"), true)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(m.last().Body); got != `{"file_name":"statement.txt","text":"1CClientBankExchange"}` {
		t.Errorf("upload body %s", got)
	}
	done, err := client.WaitForBatch(ctx, started.ID, nil)
	if err != nil || done.Running || done.FinishedAt == nil || len(m.requests()) != 4 {
		t.Errorf("%+v %v after %d requests", done, err, len(m.requests()))
	}
	if _, err := client.UploadBankStatement(ctx, "s.pdf", []byte{1, 2, 3}, false); err != nil {
		t.Fatal(err)
	}
	if got := string(m.last().Body); got != `{"file":"AQID","file_name":"s.pdf"}` {
		t.Errorf("binary upload body %s", got)
	}

	// A batch without the running flag is not taken for finished.
	m = newMock(t, respond(200, job("")))
	var decErr *DecodeError
	if _, err := m.client(nil).WaitForBatch(ctx, "bat_1", nil); !errors.As(err, &decErr) {
		t.Errorf("missing running flag: %T %v", err, err)
	}
}

// ---- money (beyond the shared vectors) --------------------------------------------------------

func TestMoneyExactTypes(t *testing.T) {
	for in, want := range map[string]int64{"1.005": 101, "0.285": 29, "-1.005": -101, "1/3": 33, "2/3": 67, "-1/8": -13, "92233720368547758.07": math.MaxInt64} {
		rat, ok := new(big.Rat).SetString(in)
		if !ok {
			t.Fatal(in)
		}
		if got, err := ToTiynRat(rat); err != nil || got != want {
			t.Errorf("ToTiynRat(%s) = %d, %v; want %d", in, got, err, want)
		}
	}
	var verr *ValidationError
	if _, err := ToTiynRat(nil); !errors.As(err, &verr) {
		t.Errorf("nil: %v", err)
	}
	huge, _ := new(big.Rat).SetString("1e30")
	if _, err := ToTiynRat(huge); !errors.As(err, &verr) {
		t.Errorf("overflow: %v", err)
	}
	if got := TiynToDecimal(math.MinInt64); got != "-92233720368547758.08" {
		t.Errorf("TiynToDecimal(min) = %s", got)
	}
	if got := TiynToDecimal(-5); got != "-0.05" {
		t.Errorf("TiynToDecimal(-5) = %s", got)
	}
}

// ---- regressions found by the independent review ----------------------------------------------

// A stream that ends without its done event is not a finished turn.
func TestAccumulateFailsWhenTheStreamEndsBeforeDone(t *testing.T) {
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sseEvent(1, "text", `{"type":"text","delta":"par"}`))
	})
	stream, err := m.client(nil).Chat.CreateStream(context.Background(), ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	turn, err := stream.Accumulate(nil)
	var decErr *DecodeError
	if !errors.As(err, &decErr) || !errors.Is(err, ErrStreamIncomplete) {
		t.Fatalf("truncated stream: %T %v", err, err)
	}
	if turn == nil || turn.Text != "par" || turn.Done != nil || turn.StopReason != "" {
		t.Errorf("the partial turn must still be returned with the error: %+v", turn)
	}

	// Iterating the raw events of the same truncated stream simply ends.
	stream, err = m.client(nil).Chat.CreateStream(context.Background(), ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, err := range stream.Events() {
		if err != nil {
			t.Fatalf("raw iteration: %v", err)
		}
		n++
	}
	if n != 1 {
		t.Errorf("%d events", n)
	}
}

// A 2xx answer to a streaming call that is not an event stream is a decode error, not a stream
// without events.
func TestStreamRejectsASuccessThatIsNotAnEventStream(t *testing.T) {
	for _, contentType := range []string{"application/json", "text/html; charset=utf-8", ""} {
		m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.Header()["Content-Type"] = nil // keep net/http from sniffing one
			if contentType != "" {
				w.Header().Set("Content-Type", contentType)
			}
			w.Header().Set("x-request-id", "req_json")
			_, _ = io.WriteString(w, `{"answer":"42","dialog_id":"d"}`)
		})
		stream, err := m.client(nil).Chat.CreateStream(context.Background(), ChatMessageRequest{})
		var decErr *DecodeError
		if stream != nil || !errors.As(err, &decErr) {
			t.Fatalf("Content-Type %q: stream %v, error %T %v", contentType, stream, err, err)
		}
		if decErr.StatusCode != 200 || decErr.RequestID != "req_json" || !strings.Contains(string(decErr.Body), `"answer"`) || len(m.requests()) != 1 {
			t.Errorf("Content-Type %q: %+v after %d requests", contentType, decErr, len(m.requests()))
		}
	}
	// The media type is compared without its parameters and without regard to case.
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.Header().Set("Content-Type", "Text/Event-Stream; charset=utf-8")
		_, _ = io.WriteString(w, sseEvent(1, "done", `{"type":"done","stop_reason":"end_turn"}`))
	})
	stream, err := m.client(nil).Chat.CreateStream(context.Background(), ChatMessageRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if turn, err := stream.Accumulate(nil); err != nil || turn.StopReason != "end_turn" {
		t.Errorf("event stream with parameters: %+v %v", turn, err)
	}
}

// Retry-After that asks for no wait (zero, a moment in the past) must not turn the retries into
// an immediate loop: the computed backoff applies.
func TestRetryAfterZeroOrPastUsesTheBackoff(t *testing.T) {
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	for name, header := range map[string][2]string{
		"zero":           {"Retry-After", "0"},
		"zero fraction":  {"Retry-After", "0.0"},
		"negative":       {"Retry-After", "-3"},
		"past date":      {"Retry-After", past},
		"zero in ms":     {"retry-after-ms", "0"},
		"negative in ms": {"retry-after-ms", "-10"},
	} {
		m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
			w.Header().Set(header[0], header[1])
			w.WriteHeader(503)
			_, _ = io.WriteString(w, errorBody)
		})
		var sleeps []time.Duration
		_, err := m.client(&sleeps, WithMaxRetries(3)).Workspaces.List(context.Background())
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.HasRetryAfter || len(m.requests()) != 4 || len(sleeps) != 3 {
			t.Fatalf("%s: %v, %d requests, slept %v", name, err, len(m.requests()), sleeps)
		}
		for attempt, d := range sleeps {
			lo := time.Duration(float64(backoffBase) * math.Pow(2, float64(attempt)) * 0.75)
			if d < lo {
				t.Errorf("%s: pause %d was %v, want the backoff (at least %v)", name, attempt, d, lo)
			}
		}
	}
}

// The loop guard of the auto-pager remembers every cursor, not only the previous one.
func TestPaginationStopsOnACursorCycle(t *testing.T) {
	cursors := []string{"A", "B", "A", "B", "A"}
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		_, _ = io.WriteString(w, proposalsPage([]string{fmt.Sprintf("P-%d", n)}, cursors[min(n-1, len(cursors)-1)]))
	})
	var items int
	var last error
	for _, err := range m.client(nil).Proposals.ListAll(context.Background(), nil) {
		if err != nil {
			last = err
			continue
		}
		items++
		if items > 10 {
			t.Fatal("the iteration does not stop")
		}
	}
	var pageErr *PaginationError
	if !errors.As(last, &pageErr) || pageErr.Cursor != "A" || items != 3 || len(m.requests()) != 3 {
		t.Errorf("A→B→A: %d items, %d requests, error %v", items, len(m.requests()), last)
	}
}

// The idle timeout measures the silence of the server, not the time the caller takes to handle
// an event.
func TestStreamIdleTimeoutIgnoresASlowConsumer(t *testing.T) {
	m := newMock(t, func(w http.ResponseWriter, r *http.Request, _ int) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 1; i <= 40; i++ {
			_, _ = io.WriteString(w, sseEvent(i, "text", `{"type":"text","delta":"a"}`))
			w.(http.Flusher).Flush()
			select {
			case <-r.Context().Done():
				return
			case <-time.After(50 * time.Millisecond): // never silent for longer than this
			}
		}
	})
	stream, err := m.client(nil).Chat.CreateStream(context.Background(), ChatMessageRequest{}, WithStreamIdleTimeout(250*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond) // a slow start counts as little as a slow loop body
	n := 0
	for _, err := range stream.Events() {
		if err != nil {
			t.Fatalf("after %d events: %T %v", n, err, err)
		}
		if n++; n >= 4 {
			break
		}
		time.Sleep(400 * time.Millisecond)
	}
	if n != 4 {
		t.Errorf("%d events", n)
	}
}

// An empty value means "not set": the README passes os.Getenv results straight to the options.
func TestEmptyBaseURLAndAPIKeyKeepTheDefaults(t *testing.T) {
	m := newMock(t, respond(200, `{"data":[]}`))
	t.Setenv("TUNGO_BASE_URL", m.srv.URL)
	t.Setenv("TUNGO_API_KEY", "tg_live_FROM_ENV")
	client := NewClient(WithAPIKey(""), WithBaseURL(""), WithWorkspaceID("ws_1"))
	if _, err := client.Workspaces.List(context.Background(), WithBaseURL(""), WithAPIKey("")); err != nil {
		t.Fatal(err)
	}
	if got := m.last().Header.Get("x-api-key"); got != "tg_live_FROM_ENV" {
		t.Errorf("key %q", got)
	}
	t.Setenv("TUNGO_BASE_URL", "")
	if got := NewClient(WithBaseURL("")).cfg.baseURL; got != DefaultBaseURL {
		t.Errorf("base URL %q", got)
	}
	// A real value still wins, on the client and on one call.
	if _, err := NewClient(WithAPIKey("tg_live_OTHER"), WithWorkspaceID("ws_1")).Workspaces.List(context.Background(), WithBaseURL(m.srv.URL)); err != nil {
		t.Fatal(err)
	}
	if got := m.last().Header.Get("x-api-key"); got != "tg_live_OTHER" {
		t.Errorf("key %q", got)
	}
}

// A poll that fails for a passing reason does not end the wait; a permanent error does.
func TestWaitToleratesTransientPollFailures(t *testing.T) {
	ctx := context.Background()
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		switch n {
		case 2, 3:
			w.WriteHeader(503)
			_, _ = io.WriteString(w, errorBody)
		case 5:
			w.WriteHeader(429)
		case 1, 4:
			_, _ = io.WriteString(w, runJSON(`"running"`))
		default:
			_, _ = io.WriteString(w, runJSON(`"done"`))
		}
	})
	run, err := m.client(nil, WithMaxRetries(0)).WaitForRun(ctx, "run_1", nil)
	if err != nil || run.State != AutomationRunDetailStateDone || len(m.requests()) != 6 {
		t.Fatalf("run: %+v %v after %d requests", run, err, len(m.requests()))
	}

	batch := `{"id":"bat_1","kind":"bank_statement","title":"t","started_at":"2026-11-09T10:00:00Z","events":3,"running":false}`
	m = newMock(t, func(w http.ResponseWriter, _ *http.Request, n int) {
		if n == 1 {
			w.WriteHeader(502)
			return
		}
		_, _ = io.WriteString(w, batch)
	})
	if job, err := m.client(nil, WithMaxRetries(0)).WaitForBatch(ctx, "bat_1", nil); err != nil || job.ID != "bat_1" || len(m.requests()) != 2 {
		t.Fatalf("batch: %+v %v after %d requests", job, err, len(m.requests()))
	}

	// A server that keeps failing ends with the deadline, as a local timeout.
	m = newMock(t, respond(503, errorBody))
	_, err = NewClient(WithAPIKey("k"), WithBaseURL(m.srv.URL), WithCompanyID("co_1"), WithMaxRetries(0)).
		WaitForRun(ctx, "run_1", &WaitOptions{Timeout: 150 * time.Millisecond, PollInterval: 10 * time.Millisecond})
	var timeoutErr *TimeoutError
	if !errors.As(err, &timeoutErr) || len(m.requests()) < 2 {
		t.Errorf("deadline: %T %v after %d polls", err, err, len(m.requests()))
	}

	// Permanent errors still end the wait at once.
	for _, status := range []int{400, 401, 403, 404} {
		m = newMock(t, respond(status, errorBody))
		var apiErr *APIError
		if _, err = m.client(nil).WaitForBatch(ctx, "bat_1", nil); !errors.As(err, &apiErr) || len(m.requests()) != 1 {
			t.Errorf("%d: %v after %d requests", status, err, len(m.requests()))
		}
	}
}

// The body of an error response is read up to a limit.
func TestErrorBodyIsCapped(t *testing.T) {
	huge := strings.Repeat("x", 3*maxErrorBody)
	m := newMock(t, func(w http.ResponseWriter, _ *http.Request, _ int) {
		w.WriteHeader(500)
		_, _ = io.WriteString(w, huge)
	})
	var apiErr *APIError
	_, err := m.client(nil, WithMaxRetries(0)).Workspaces.List(context.Background())
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 500 || len(apiErr.Body) != maxErrorBody {
		t.Errorf("buffered call: %T, body of %d bytes", err, len(apiErr.Body))
	}
	apiErr = nil
	_, err = m.client(nil, WithMaxRetries(0)).Chat.CreateStream(context.Background(), ChatMessageRequest{})
	if !errors.As(err, &apiErr) || apiErr.StatusCode != 500 || len(apiErr.Body) != maxErrorBody {
		t.Errorf("stream call: %T, body of %d bytes", err, len(apiErr.Body))
	}
	// A successful body is not limited.
	m = newMock(t, respond(200, `{"data":[],"pad":"`+huge+`"}`))
	var resp *Response
	if _, err := m.client(nil).Workspaces.List(context.Background(), WithResponseInto(&resp)); err != nil || len(resp.Body) < len(huge) {
		t.Errorf("success: %v, %d bytes", err, len(resp.Body))
	}
}
