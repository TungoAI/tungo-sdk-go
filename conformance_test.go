package tungo

// The shared conformance vectors (conformance/vectors.json) are the same file in all ten Tungo
// SDKs. This test runs every section of it against this SDK.

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type vecExpectTiyn struct {
	Tiyn  *string `json:"tiyn"`
	Error bool    `json:"error"`
}

type vecWireError struct {
	Type      string         `json:"type"`
	Code      *string        `json:"code"`
	Message   string         `json:"message"`
	RequestID *string        `json:"request_id"`
	Extra     map[string]any `json:"extra"`
}

type vecFieldError struct {
	Path    string `json:"path"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type vecSSEEvent struct {
	Event string  `json:"event"`
	Data  string  `json:"data"`
	ID    *string `json:"id"`
	Retry *int    `json:"retry"`
}

type vectorsFile struct {
	Version int `json:"version"`

	WebhookVerification struct {
		DefaultToleranceSeconds int64 `json:"default_tolerance_seconds"`
		Cases                   []struct {
			Name             string            `json:"name"`
			Secret           string            `json:"secret"`
			Headers          map[string]string `json:"headers"`
			Body             string            `json:"body"`
			Now              int64             `json:"now"`
			ToleranceSeconds *int64            `json:"tolerance_seconds"`
			Expect           struct {
				Valid     bool   `json:"valid"`
				EventType string `json:"event_type"`
				Reason    string `json:"reason"`
			} `json:"expect"`
		} `json:"cases"`
	} `json:"webhook_verification"`

	HookSignature struct {
		Sign []struct {
			Name       string `json:"name"`
			Secret     string `json:"secret"`
			BodyBase64 string `json:"body_base64"`
			Expect     struct {
				Hex    string `json:"hex"`
				Header string `json:"header"`
				Error  bool   `json:"error"`
			} `json:"expect"`
		} `json:"sign"`
		Verify []struct {
			Name       string `json:"name"`
			Secret     string `json:"secret"`
			BodyBase64 string `json:"body_base64"`
			Signature  string `json:"signature"`
			Expect     struct {
				Valid bool `json:"valid"`
			} `json:"expect"`
		} `json:"verify"`
	} `json:"hook_signature"`

	Money struct {
		ToTiyn []struct {
			Name   string        `json:"name"`
			Input  string        `json:"input"`
			Expect vecExpectTiyn `json:"expect"`
		} `json:"to_tiyn"`
		ToTiynFloat []struct {
			Name    string        `json:"name"`
			Input   *float64      `json:"input"`
			Special string        `json:"special"`
			Expect  vecExpectTiyn `json:"expect"`
		} `json:"to_tiyn_float"`
		FormatTiyn []struct {
			Name     string  `json:"name"`
			Tiyn     string  `json:"tiyn"`
			Currency *string `json:"currency"`
			Expect   struct {
				Text string `json:"text"`
			} `json:"expect"`
		} `json:"format_tiyn"`
	} `json:"money"`

	BinIin []struct {
		Name   string `json:"name"`
		Input  string `json:"input"`
		Expect struct {
			Valid bool `json:"valid"`
		} `json:"expect"`
	} `json:"bin_iin"`

	Errors struct {
		Cases []struct {
			Name    string            `json:"name"`
			Status  int               `json:"status"`
			Headers map[string]string `json:"headers"`
			Body    string            `json:"body"`
			Expect  struct {
				Envelope   bool            `json:"envelope"`
				Class      string          `json:"class"`
				Type       string          `json:"type"`
				Code       *string         `json:"code"`
				Message    *string         `json:"message"`
				RequestID  *string         `json:"request_id"`
				RetryAfter *float64        `json:"retry_after"`
				Fields     []vecFieldError `json:"fields"`
				Extra      map[string]any  `json:"extra"`
			} `json:"expect"`
		} `json:"cases"`
		StatusFallback []struct {
			Status int `json:"status"`
			Expect struct {
				Type string `json:"type"`
			} `json:"expect"`
		} `json:"status_fallback"`
	} `json:"errors"`

	RetryAfter struct {
		MaxSleepSeconds float64 `json:"max_sleep_seconds"`
		Cases           []struct {
			Name    string            `json:"name"`
			Headers map[string]string `json:"headers"`
			Now     string            `json:"now"`
			Expect  struct {
				Seconds *float64 `json:"seconds"`
				Sleep   bool     `json:"sleep"`
			} `json:"expect"`
		} `json:"cases"`
	} `json:"retry_after"`

	Backoff struct {
		BaseSeconds float64 `json:"base_seconds"`
		MaxSeconds  float64 `json:"max_seconds"`
		JitterMin   float64 `json:"jitter_min"`
		JitterMax   float64 `json:"jitter_max"`
		Cases       []struct {
			Name    string `json:"name"`
			Attempt int    `json:"attempt"`
			Expect  struct {
				MinSeconds float64 `json:"min_seconds"`
				MaxSeconds float64 `json:"max_seconds"`
			} `json:"expect"`
		} `json:"cases"`
	} `json:"backoff"`

	RetryPolicy []struct {
		Name           string `json:"name"`
		Method         string `json:"method"`
		IdempotencyKey bool   `json:"idempotency_key"`
		CallerKey      bool   `json:"caller_key"`
		Cause          any    `json:"cause"`
		Expect         struct {
			Retry bool `json:"retry"`
		} `json:"expect"`
	} `json:"retry_policy"`

	PathSegment []struct {
		Name   string `json:"name"`
		Input  string `json:"input"`
		Expect struct {
			Encoded *string `json:"encoded"`
			Error   bool    `json:"error"`
		} `json:"expect"`
	} `json:"path_segment"`

	Query []struct {
		Name   string `json:"name"`
		Params []struct {
			Name  string          `json:"name"`
			Type  string          `json:"type"`
			Value json.RawMessage `json:"value"`
		} `json:"params"`
		Expect struct {
			Query *string  `json:"query"`
			AnyOf []string `json:"any_of"`
		} `json:"expect"`
	} `json:"query"`

	SSE []struct {
		Name         string   `json:"name"`
		RawBase64    string   `json:"raw_base64"`
		ChunksBase64 []string `json:"chunks_base64"`
		Expect       struct {
			Events []vecSSEEvent `json:"events"`
		} `json:"expect"`
	} `json:"sse"`

	ChatStream []struct {
		Name    string            `json:"name"`
		Status  int               `json:"status"`
		Headers map[string]string `json:"headers"`
		Raw     string            `json:"raw"`
		Expect  struct {
			DialogID   string         `json:"dialog_id"`
			TurnNo     int64          `json:"turn_no"`
			Text       string         `json:"text"`
			StopReason *string        `json:"stop_reason"`
			EventCount int            `json:"event_count"`
			EventTypes []string       `json:"event_types"`
			CostTiyn   *int64         `json:"cost_tiyn"`
			Usage      map[string]any `json:"usage"`
			ChoiceID   *string        `json:"choice_id"`
			Error      *vecWireError  `json:"error"`
		} `json:"expect"`
	} `json:"chat_stream"`
}

func loadVectors(t *testing.T) *vectorsFile {
	t.Helper()
	raw, err := os.ReadFile("conformance/vectors.json")
	if err != nil {
		t.Fatalf("read the conformance vectors: %v", err)
	}
	var v vectorsFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("decode the conformance vectors: %v", err)
	}
	if v.Version != 2 {
		t.Fatalf("conformance vectors version %d, this test understands version 2", v.Version)
	}
	return &v
}

func httpHeader(m map[string]string) http.Header {
	h := http.Header{}
	for k, v := range m {
		h.Set(k, v)
	}
	return h
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// sameJSON compares two values through their JSON form (numbers as float64).
func sameJSON(t *testing.T, got, want any) bool {
	t.Helper()
	norm := func(v any) any {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		var out any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		return out
	}
	return reflect.DeepEqual(norm(got), norm(want))
}

func TestConformanceWebhookVerification(t *testing.T) {
	v := loadVectors(t)
	if got := int64(DefaultWebhookTolerance / time.Second); got != v.WebhookVerification.DefaultToleranceSeconds {
		t.Fatalf("default tolerance %d s, vectors say %d s", got, v.WebhookVerification.DefaultToleranceSeconds)
	}
	if len(v.WebhookVerification.Cases) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range v.WebhookVerification.Cases {
		t.Run(c.Name, func(t *testing.T) {
			opts := []WebhookOption{WithWebhookClock(func() time.Time { return time.Unix(c.Now, 0) })}
			if c.ToleranceSeconds != nil {
				opts = append(opts, WithWebhookTolerance(time.Duration(*c.ToleranceSeconds)*time.Second))
			}
			// The headers are given both through http.Header and as a raw map with the vector's
			// own spelling, which is what a non-net/http framework would hand over.
			raw := http.Header{}
			for k, val := range c.Headers {
				raw[k] = []string{val}
			}
			for _, h := range []http.Header{httpHeader(c.Headers), raw} {
				event, err := VerifyWebhook([]byte(c.Body), h, c.Secret, opts...)
				if c.Expect.Valid {
					if err != nil {
						t.Fatalf("want valid, got %v", err)
					}
					if event.Tag() != c.Expect.EventType {
						t.Fatalf("event type %q, want %q", event.Tag(), c.Expect.EventType)
					}
					if _, unknown := event.Value.(*UnknownVariant); unknown {
						t.Fatalf("event %q decoded as an unknown variant", c.Expect.EventType)
					}
					continue
				}
				var verr *WebhookVerificationError
				if !errors.As(err, &verr) {
					t.Fatalf("want *WebhookVerificationError, got %T %v", err, err)
				}
				if string(verr.Reason) != c.Expect.Reason {
					t.Fatalf("reason %q, want %q", verr.Reason, c.Expect.Reason)
				}
				if event.Value != nil {
					t.Fatal("a rejected webhook must not return an event")
				}
			}
		})
	}
}

func TestConformanceHookSignature(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.HookSignature.Sign {
		body, err := base64.StdEncoding.DecodeString(c.BodyBase64)
		if err != nil {
			t.Fatal(err)
		}
		got, err := SignHook(body, c.Secret)
		if c.Expect.Error {
			var verr *ValidationError
			if !errors.As(err, &verr) {
				t.Errorf("%s: want *ValidationError, got %v", c.Name, err)
			}
			continue
		}
		if err != nil || got != c.Expect.Header || strings.TrimPrefix(got, "sha256=") != c.Expect.Hex {
			t.Errorf("%s: SignHook = %q, %v; want %q", c.Name, got, err, c.Expect.Header)
		}
	}
	for _, c := range v.HookSignature.Verify {
		body, err := base64.StdEncoding.DecodeString(c.BodyBase64)
		if err != nil {
			t.Fatal(err)
		}
		if got := VerifyHookSignature(body, c.Signature, c.Secret); got != c.Expect.Valid {
			t.Errorf("%s: VerifyHookSignature = %v, want %v", c.Name, got, c.Expect.Valid)
		}
	}
	if len(v.HookSignature.Sign) == 0 || len(v.HookSignature.Verify) == 0 {
		t.Fatal("no cases")
	}
}

func checkTiyn(t *testing.T, name string, got int64, err error, want vecExpectTiyn) {
	t.Helper()
	if want.Error {
		var verr *ValidationError
		if !errors.As(err, &verr) {
			t.Errorf("%s: want *ValidationError, got %d, %v", name, got, err)
		}
		return
	}
	if err != nil || strconv.FormatInt(got, 10) != *want.Tiyn {
		t.Errorf("%s: got %d, %v; want %s", name, got, err, *want.Tiyn)
	}
}

func TestConformanceMoney(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.Money.ToTiyn {
		got, err := ToTiyn(c.Input)
		checkTiyn(t, "ToTiyn "+c.Name, got, err, c.Expect)
		// The exact rational path must agree wherever the input is a number at all.
		if rat, ok := new(big.Rat).SetString(c.Input); ok && !strings.ContainsAny(c.Input, "eE/+ _x") && !c.Expect.Error {
			got, err := ToTiynRat(rat)
			checkTiyn(t, "ToTiynRat "+c.Name, got, err, c.Expect)
		}
	}
	for _, c := range v.Money.ToTiynFloat {
		var in float64
		switch c.Special {
		case "NaN":
			in = math.NaN()
		case "Infinity":
			in = math.Inf(1)
		case "-Infinity":
			in = math.Inf(-1)
		default:
			in = *c.Input
		}
		got, err := ToTiynFloat(in)
		checkTiyn(t, "ToTiynFloat "+c.Name, got, err, c.Expect)
	}
	for _, c := range v.Money.FormatTiyn {
		tiyn, err := strconv.ParseInt(c.Tiyn, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if got := FormatTiyn(tiyn, deref(c.Currency)); got != c.Expect.Text {
			t.Errorf("FormatTiyn %s: %q, want %q", c.Name, got, c.Expect.Text)
		}
		// TiynToDecimal and ToTiyn are inverse for every int64.
		if back, err := ToTiyn(TiynToDecimal(tiyn)); err != nil || back != tiyn {
			t.Errorf("ToTiyn(TiynToDecimal(%d)) = %d, %v", tiyn, back, err)
		}
	}
	if len(v.Money.ToTiyn) == 0 || len(v.Money.ToTiynFloat) == 0 || len(v.Money.FormatTiyn) == 0 {
		t.Fatal("no cases")
	}
}

func TestConformanceBinIin(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.BinIin {
		if got := IsValidBINIIN(c.Input); got != c.Expect.Valid {
			t.Errorf("%s: IsValidBINIIN(%q) = %v, want %v", c.Name, c.Input, got, c.Expect.Valid)
		}
	}
	if len(v.BinIin) == 0 {
		t.Fatal("no cases")
	}
}

func TestConformanceErrors(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.Errors.Cases {
		t.Run(c.Name, func(t *testing.T) {
			// End to end: the error comes out of a real call against a server that answers with
			// exactly this status, these headers and this body.
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				for k, val := range c.Headers {
					w.Header().Set(k, val)
				}
				w.WriteHeader(c.Status)
				_, _ = io.WriteString(w, c.Body)
			}))
			defer srv.Close()
			client := NewClient(WithAPIKey("tg_test_key"), WithBaseURL(srv.URL), WithMaxRetries(0))
			_, err := client.Workspaces.List(context.Background())
			var e *APIError
			if !errors.As(err, &e) {
				t.Fatalf("want *APIError, got %T %v", err, err)
			}
			var sdkErr Error
			if !errors.As(err, &sdkErr) {
				t.Fatal("an *APIError must be a tungo.Error")
			}
			want := c.Expect
			if e.StatusCode != c.Status || string(e.Kind) != want.Class || e.Type != want.Type || KindOf(err) != e.Kind {
				t.Errorf("status %d kind %q type %q; want %d %q %q", e.StatusCode, e.Kind, e.Type, c.Status, want.Class, want.Type)
			}
			if (e.Detail != nil) != want.Envelope {
				t.Errorf("envelope detected = %v, want %v", e.Detail != nil, want.Envelope)
			}
			if e.Code != deref(want.Code) {
				t.Errorf("code %q, want %q", e.Code, deref(want.Code))
			}
			if want.Message != nil && e.Message != *want.Message {
				t.Errorf("message %q, want %q", e.Message, *want.Message)
			}
			if e.Message == "" {
				t.Error("the message must never be empty")
			}
			if e.RequestID != deref(want.RequestID) {
				t.Errorf("request id %q, want %q", e.RequestID, deref(want.RequestID))
			}
			if e.HasRetryAfter != (want.RetryAfter != nil) || (want.RetryAfter != nil && math.Abs(e.RetryAfter.Seconds()-*want.RetryAfter) > 1e-9) {
				t.Errorf("retry after %v (%v), want %v", e.RetryAfter, e.HasRetryAfter, want.RetryAfter)
			}
			gotFields := []vecFieldError{}
			for _, f := range e.Fields {
				gotFields = append(gotFields, vecFieldError{Path: f.Path, Code: string(f.Code), Message: f.Message})
			}
			if !reflect.DeepEqual(gotFields, want.Fields) {
				t.Errorf("fields %+v, want %+v", gotFields, want.Fields)
			}
			gotExtra := map[string]json.RawMessage{}
			for k, val := range e.Extra {
				gotExtra[k] = val
			}
			if !sameJSON(t, gotExtra, want.Extra) {
				t.Errorf("extra %s, want %v", gotExtra, want.Extra)
			}
			if string(e.Body) != c.Body {
				t.Errorf("raw body %q, want %q", e.Body, c.Body)
			}
			for k, val := range c.Headers {
				if e.Header.Get(k) != val {
					t.Errorf("response header %s = %q, want %q", k, e.Header.Get(k), val)
				}
			}
			msg := e.Error()
			if !strings.Contains(msg, strconv.Itoa(c.Status)) || (e.RequestID != "" && !strings.Contains(msg, e.RequestID)) || (e.Code != "" && !strings.Contains(msg, e.Code)) {
				t.Errorf("Error() = %q must mention the status, the code and the request id", msg)
			}
			if strings.Contains(msg, "tg_test_key") {
				t.Error("the error message leaks the API key")
			}
		})
	}
	for _, c := range v.Errors.StatusFallback {
		if got := kindForStatus(c.Status); string(got) != c.Expect.Type {
			t.Errorf("status %d falls back to %q, want %q", c.Status, got, c.Expect.Type)
		}
	}
	if len(v.Errors.Cases) == 0 || len(v.Errors.StatusFallback) == 0 {
		t.Fatal("no cases")
	}
}

func TestConformanceRetryAfter(t *testing.T) {
	v := loadVectors(t)
	if maxRetryAfter.Seconds() != v.RetryAfter.MaxSleepSeconds {
		t.Fatalf("max sleep %v, vectors say %v s", maxRetryAfter, v.RetryAfter.MaxSleepSeconds)
	}
	for _, c := range v.RetryAfter.Cases {
		now, err := time.Parse(time.RFC3339, c.Now)
		if err != nil {
			t.Fatal(err)
		}
		d, ok := parseRetryAfter(httpHeader(c.Headers), now)
		// A delay of zero (the header says 0, or names a moment that has passed) is not a usable
		// value: the SDK waits its computed backoff, exactly as when the header is absent.
		usable := c.Expect.Seconds != nil && *c.Expect.Seconds > 0
		if ok != usable || (ok && math.Abs(d.Seconds()-*c.Expect.Seconds) > 1e-9) {
			t.Errorf("%s: parsed %v (%v), want %v", c.Name, d, ok, c.Expect.Seconds)
		}
		if sleeps := !ok || d <= maxRetryAfter; sleeps != c.Expect.Sleep {
			t.Errorf("%s: sleep = %v, want %v", c.Name, sleeps, c.Expect.Sleep)
		}
	}
	if len(v.RetryAfter.Cases) == 0 {
		t.Fatal("no cases")
	}
}

func TestConformanceBackoff(t *testing.T) {
	v := loadVectors(t)
	b := v.Backoff
	if backoffBase.Seconds() != b.BaseSeconds || backoffMax.Seconds() != b.MaxSeconds || b.JitterMin != 0.75 || b.JitterMax != 1.0 {
		t.Fatalf("backoff constants differ from the vectors: %+v", b)
	}
	for _, c := range b.Cases {
		lo, hi := math.Inf(1), math.Inf(-1)
		for i := 0; i < 2000; i++ {
			d := backoffDelay(c.Attempt).Seconds()
			if d < c.Expect.MinSeconds-1e-9 || d > c.Expect.MaxSeconds+1e-9 {
				t.Fatalf("%s: delay %v s outside [%v, %v]", c.Name, d, c.Expect.MinSeconds, c.Expect.MaxSeconds)
			}
			lo, hi = math.Min(lo, d), math.Max(hi, d)
		}
		// The delay is jittered: 2000 samples must spread over most of the interval.
		if span := c.Expect.MaxSeconds - c.Expect.MinSeconds; hi-lo < span/2 {
			t.Errorf("%s: no jitter: samples only span [%v, %v]", c.Name, lo, hi)
		}
	}
	if len(b.Cases) == 0 {
		t.Fatal("no cases")
	}
}

// TestConformanceRetryPolicy drives every case through a real call: the server (or the
// transport) fails every attempt and the test counts the attempts.
func TestConformanceRetryPolicy(t *testing.T) {
	v := loadVectors(t)
	if len(v.RetryPolicy) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range v.RetryPolicy {
		t.Run(c.Name, func(t *testing.T) {
			var attempts atomic.Int32
			status := 0
			cause := ""
			switch x := c.Cause.(type) {
			case float64:
				status = int(x)
			case string:
				cause = x
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				if r.Method != c.Method {
					t.Errorf("method %s, want %s", r.Method, c.Method)
				}
				if has := r.Header.Get("Idempotency-Key") != ""; has != c.IdempotencyKey {
					t.Errorf("Idempotency-Key present = %v, want %v", has, c.IdempotencyKey)
				}
				switch cause {
				case "connection_error":
					conn, _, err := w.(http.Hijacker).Hijack()
					if err == nil {
						conn.Close() // drop the connection without an answer
					}
				case "timeout":
					_, _ = io.Copy(io.Discard, r.Body) // lets the server notice the client going away
					select {
					case <-r.Context().Done():
					case <-time.After(5 * time.Second):
					}
				default:
					w.WriteHeader(status)
				}
			}))
			defer srv.Close()

			noSleep := func(c *config) { c.sleep = func(context.Context, time.Duration) error { return nil } }
			opts := []Option{WithAPIKey("k"), WithBaseURL(srv.URL), WithCompanyID("co_1"), WithWorkspaceID("ws_1"), noSleep}
			if cause == "timeout" {
				opts = append(opts, WithTimeout(40*time.Millisecond))
			}
			client := NewClient(opts...)
			ctx := context.Background()
			var err error
			switch {
			case c.Method == "GET":
				_, err = client.Workspaces.List(ctx)
			case c.Method == "PUT":
				_, err = client.Automations.PutState(ctx, AutomationStateInput{})
			case c.Method == "DELETE":
				err = client.Webhooks.DeleteEndpoint(ctx, "ep_1")
			case c.Method == "POST" && c.IdempotencyKey:
				_, err = client.Automations.RotateHook(ctx, "hook_1") // the SDK generates the key
			case c.Method == "POST" && c.CallerKey:
				// chat.stop declares no Idempotency-Key: the caller's key does not make it repeatable.
				_, err = client.Chat.Stop(ctx, "dlg_1", WithIdempotencyKey("caller-key"), WithHeader("Idempotency-Key", "header-key"))
			case c.Method == "POST":
				_, err = client.Chat.Stop(ctx, "dlg_1")
			case c.Method == "PATCH" && c.IdempotencyKey:
				// No PATCH operation of the contract declares Idempotency-Key: only the policy is checked.
				if retry := safeToRepeat(c.Method, true) && (cause != "" || retryableStatus(status)); retry != c.Expect.Retry {
					t.Errorf("policy says retry = %v, want %v", retry, c.Expect.Retry)
				}
				return
			case c.Method == "PATCH" && c.CallerKey:
				_, err = client.Chat.Update(ctx, "dlg_1", ChatDialogPatch{}, WithIdempotencyKey("caller-key"))
			case c.Method == "PATCH":
				_, err = client.Chat.Update(ctx, "dlg_1", ChatDialogPatch{})
			default:
				t.Fatalf("no operation for method %s", c.Method)
			}
			if err == nil {
				t.Fatal("the call must fail")
			}
			want := int32(1)
			if c.Expect.Retry {
				want = 1 + DefaultMaxRetries
			}
			if got := attempts.Load(); got != want {
				t.Errorf("%d attempts, want %d (err: %v)", got, want, err)
			}
			var apiErr *APIError
			var connErr *ConnectionError
			var timeoutErr *TimeoutError
			switch cause {
			case "connection_error":
				if !errors.As(err, &connErr) {
					t.Errorf("want *ConnectionError, got %T %v", err, err)
				}
			case "timeout":
				if !errors.As(err, &timeoutErr) || !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("want *TimeoutError wrapping context.DeadlineExceeded, got %T %v", err, err)
				}
			default:
				if !errors.As(err, &apiErr) || apiErr.StatusCode != status {
					t.Errorf("want *APIError %d, got %T %v", status, err, err)
				}
			}
			// The pure decision function agrees.
			retry := safeToRepeat(c.Method, c.IdempotencyKey) && (cause != "" || retryableStatus(status))
			if retry != c.Expect.Retry {
				t.Errorf("policy says retry = %v, want %v", retry, c.Expect.Retry)
			}
		})
	}
}

func TestConformancePathSegment(t *testing.T) {
	v := loadVectors(t)
	var seen atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.Store(r.RequestURI)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()
	client := NewClient(WithAPIKey("k"), WithBaseURL(srv.URL), WithCompanyID("co_1"))
	for _, c := range v.PathSegment {
		got, err := pathSegment("id", c.Input)
		seen.Store("")
		_, callErr := client.Chat.Get(context.Background(), c.Input)
		if c.Expect.Error {
			var verr *ValidationError
			if !errors.As(err, &verr) || !errors.As(callErr, &verr) {
				t.Errorf("%s: want *ValidationError, got %v / %v", c.Name, err, callErr)
			}
			if seen.Load() != "" {
				t.Errorf("%s: a request was sent (%v) although the id is invalid", c.Name, seen.Load())
			}
			continue
		}
		if err != nil || got != *c.Expect.Encoded {
			t.Errorf("%s: pathSegment(%q) = %q, %v; want %q", c.Name, c.Input, got, err, *c.Expect.Encoded)
		}
		if want := "/v1/companies/co_1/chat/" + *c.Expect.Encoded; callErr != nil || seen.Load() != want {
			t.Errorf("%s: request URI %v (%v), want %q", c.Name, seen.Load(), callErr, want)
		}
	}
	if len(v.PathSegment) == 0 {
		t.Fatal("no cases")
	}
}

func TestConformanceQuery(t *testing.T) {
	v := loadVectors(t)
	for _, c := range v.Query {
		q := url.Values{}
		for _, p := range c.Params {
			if string(bytes.TrimSpace(p.Value)) == "null" {
				addQuery[string](q, p.Name, nil)
				continue
			}
			switch p.Type {
			case "string", "date-time":
				var s string
				mustDecode(t, p.Value, &s)
				addQuery(q, p.Name, &s)
			case "boolean":
				var b bool
				mustDecode(t, p.Value, &b)
				setQuery(q, p.Name, b)
			case "integer":
				var n int64
				text := strings.Trim(string(p.Value), `"`) // integers beyond 2^53 are given as strings
				n, err := strconv.ParseInt(text, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				addQuery(q, p.Name, &n)
			case "array":
				var list []string
				mustDecode(t, p.Value, &list)
				addQueryList(q, p.Name, list)
			default:
				t.Fatalf("%s: unknown parameter type %q", c.Name, p.Type)
			}
		}
		got := q.Encode()
		ok := c.Expect.Query != nil && got == *c.Expect.Query
		for _, alt := range c.Expect.AnyOf {
			ok = ok || got == alt
		}
		if !ok {
			t.Errorf("%s: query %q, want %v %v", c.Name, got, deref(c.Expect.Query), c.Expect.AnyOf)
		}
	}
	if len(v.Query) == 0 {
		t.Fatal("no cases")
	}
}

func mustDecode(t *testing.T, raw json.RawMessage, into any) {
	t.Helper()
	if err := json.Unmarshal(raw, into); err != nil {
		t.Fatal(err)
	}
}

// chunkReader returns the given chunks one Read at a time, like a network connection would.
type chunkReader struct {
	chunks [][]byte
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.chunks[0])
	if n < len(r.chunks[0]) {
		r.chunks[0] = r.chunks[0][n:]
	} else {
		r.chunks = r.chunks[1:]
	}
	return n, nil
}

func TestConformanceSSE(t *testing.T) {
	v := loadVectors(t)
	check := func(name string, chunks [][]byte, want []vecSSEEvent) {
		t.Helper()
		got := []vecSSEEvent{}
		for ev, err := range ParseSSE(&chunkReader{chunks: chunks}) {
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			id := ev.ID
			got = append(got, vecSSEEvent{Event: ev.Event, Data: ev.Data, ID: &id, Retry: ev.Retry})
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d events, want %d: %+v", name, len(got), len(want), got)
		}
		for i := range want {
			g, w := got[i], want[i]
			sameRetry := (g.Retry == nil) == (w.Retry == nil) && (g.Retry == nil || *g.Retry == *w.Retry)
			if g.Event != w.Event || g.Data != w.Data || deref(g.ID) != deref(w.ID) || !sameRetry {
				t.Errorf("%s: event %d = %+v, want %+v", name, i, g, w)
			}
		}
	}
	for _, c := range v.SSE {
		raw, err := base64.StdEncoding.DecodeString(c.RawBase64)
		if err != nil {
			t.Fatal(err)
		}
		check(c.Name+" (one chunk)", [][]byte{raw}, c.Expect.Events)
		// Byte by byte: every split point of the stream is exercised.
		single := make([][]byte, len(raw))
		for i := range raw {
			single[i] = raw[i : i+1]
		}
		check(c.Name+" (byte by byte)", single, c.Expect.Events)
		if len(c.ChunksBase64) > 0 {
			var chunks [][]byte
			for _, b64 := range c.ChunksBase64 {
				chunk, err := base64.StdEncoding.DecodeString(b64)
				if err != nil {
					t.Fatal(err)
				}
				chunks = append(chunks, chunk)
			}
			check(c.Name+" (vector chunks)", chunks, c.Expect.Events)
		}
	}
	if len(v.SSE) == 0 {
		t.Fatal("no cases")
	}
}

func TestConformanceChatStream(t *testing.T) {
	v := loadVectors(t)
	if len(v.ChatStream) == 0 {
		t.Fatal("no cases")
	}
	for _, c := range v.ChatStream {
		t.Run(c.Name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Accept") != "text/event-stream" {
					t.Errorf("Accept = %q", r.Header.Get("Accept"))
				}
				var body map[string]any
				_ = json.NewDecoder(r.Body).Decode(&body)
				if body["stream"] != true {
					t.Errorf("the streaming call must send stream:true, body %v", body)
				}
				for k, val := range c.Headers {
					w.Header().Set(k, val)
				}
				w.WriteHeader(c.Status)
				_, _ = io.WriteString(w, c.Raw)
			}))
			defer srv.Close()
			client := NewClient(WithAPIKey("k"), WithBaseURL(srv.URL), WithCompanyID("co_1"))
			stream, err := client.Chat.CreateStream(context.Background(), ChatMessageRequest{Message: Ptr("?")})
			if err != nil {
				t.Fatal(err)
			}
			var live []string
			turn, err := stream.Accumulate(func(ev ChatEvent) { live = append(live, ev.Tag()) })
			want := c.Expect

			// A stream that closes before its done event is not a finished turn: Accumulate still
			// returns what arrived (checked below), together with an error.
			if want.Error == nil && want.EventTypes != nil && !slices.Contains(want.EventTypes, "done") {
				var decErr *DecodeError
				if !errors.As(err, &decErr) || !errors.Is(err, ErrStreamIncomplete) {
					t.Fatalf("a stream without done: want a *DecodeError for ErrStreamIncomplete, got %T %v", err, err)
				}
			} else if want.Error == nil {
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
			} else {
				var turnErr *TurnError
				if !errors.As(err, &turnErr) {
					t.Fatalf("want *TurnError, got %T %v", err, err)
				}
				gotExtra := map[string]json.RawMessage{}
				for k, val := range turnErr.Extra {
					gotExtra[k] = val
				}
				if turnErr.Type != want.Error.Type || turnErr.Code != deref(want.Error.Code) || turnErr.Message != want.Error.Message ||
					turnErr.RequestID != deref(want.Error.RequestID) || !sameJSON(t, gotExtra, want.Error.Extra) || string(turnErr.Kind) != want.Error.Type {
					t.Errorf("turn error %+v (extra %s), want %+v", turnErr, gotExtra, want.Error)
				}
			}
			if turn.DialogID != want.DialogID || turn.TurnNo != want.TurnNo {
				t.Errorf("dialog %q turn %d, want %q %d (from the response headers)", turn.DialogID, turn.TurnNo, want.DialogID, want.TurnNo)
			}
			if turn.Text != want.Text {
				t.Errorf("text %q, want %q", turn.Text, want.Text)
			}
			if turn.StopReason != deref(want.StopReason) || (turn.Done == nil) != (want.StopReason == nil) {
				t.Errorf("stop reason %q (done %v), want %v", turn.StopReason, turn.Done != nil, want.StopReason)
			}
			if len(turn.Events) != want.EventCount || len(live) != want.EventCount {
				t.Errorf("%d events (%d seen live), want %d", len(turn.Events), len(live), want.EventCount)
			}
			if want.EventTypes != nil && !reflect.DeepEqual(live, want.EventTypes) {
				t.Errorf("event types %v, want %v", live, want.EventTypes)
			}
			var cost *int64
			var usage any
			choice := ""
			if turn.Done != nil {
				cost = turn.Done.CostTiyn
				if turn.Done.Usage != nil {
					usage = turn.Done.Usage
				}
				choice = deref(turn.Done.ChoiceID)
			}
			if (cost == nil) != (want.CostTiyn == nil) || (cost != nil && *cost != *want.CostTiyn) {
				t.Errorf("cost %v, want %v", cost, want.CostTiyn)
			}
			if (usage == nil) != (want.Usage == nil) || (usage != nil && !sameJSON(t, usage, want.Usage)) {
				t.Errorf("usage %+v, want %v", usage, want.Usage)
			}
			if choice != deref(want.ChoiceID) {
				t.Errorf("choice id %q, want %v", choice, want.ChoiceID)
			}
			if stream.Response().RequestID() != c.Headers["x-request-id"] {
				t.Errorf("request id %q", stream.Response().RequestID())
			}
		})
	}
}
