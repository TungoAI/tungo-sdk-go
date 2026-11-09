package tungo

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

type conformanceVectors struct {
	WebhookVerification struct {
		Secret         string `json:"secret"`
		MsgID          string `json:"msg_id"`
		Timestamp      int64  `json:"timestamp"`
		Body           string `json:"body"`
		ValidSignature string `json:"valid_signature"`
		DualSignature  string `json:"dual_signature"`
		BadSignature   string `json:"bad_signature"`
	} `json:"webhook_verification"`
	IncomingHook struct {
		Secret            string `json:"secret"`
		Body              string `json:"body"`
		HexSignature      string `json:"hex_signature"`
		PrefixedSignature string `json:"prefixed_signature"`
	} `json:"incoming_hook"`
	Money []struct {
		Tenge     float64 `json:"tenge"`
		Tiyn      int64   `json:"tiyn"`
		Formatted string  `json:"formatted"`
	} `json:"money"`
	BinIin []struct {
		Value string `json:"value"`
		Valid bool   `json:"valid"`
	} `json:"bin_iin"`
	Errors []struct {
		Status    int    `json:"status"`
		Body      string `json:"body"`
		ErrorType string `json:"error_type"`
		ErrorCode string `json:"error_code"`
		Hint      string `json:"hint"`
		RequestID string `json:"request_id"`
	} `json:"errors"`
	SSE struct {
		RawStream    string `json:"raw_stream"`
		ExpectedText string `json:"expected_text"`
		Events       []struct {
			Event string `json:"event"`
			Data  string `json:"data"`
		} `json:"events"`
	} `json:"sse"`
}

func loadVectors(t *testing.T) conformanceVectors {
	t.Helper()
	raw, err := os.ReadFile("../tungo-sdkgen/conformance/vectors.json")
	if err != nil {
		t.Fatalf("read vectors.json: %v", err)
	}
	var v conformanceVectors
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatalf("unmarshal vectors.json: %v", err)
	}
	return v
}

func TestConformanceVectors(t *testing.T) {
	v := loadVectors(t)

	// 1. Svix Webhook Verification
	headers := map[string]string{
		"svix-id":        v.WebhookVerification.MsgID,
		"svix-timestamp": "1793974568",
		"svix-signature": v.WebhookVerification.DualSignature,
	}
	env, err := VerifyWebhook(v.WebhookVerification.Body, headers, v.WebhookVerification.Secret, v.WebhookVerification.Timestamp, 300)
	if err != nil {
		t.Fatalf("VerifyWebhook failed: %v", err)
	}
	if env.Type != "proposal.written" || env.CompanyID != "co_01h455vb4pex5vsknk084sn02q" {
		t.Fatalf("unexpected webhook envelope: %+v", env)
	}
	if _, err := VerifyWebhook(v.WebhookVerification.Body+"x", headers, v.WebhookVerification.Secret, v.WebhookVerification.Timestamp, 300); err == nil {
		t.Fatal("expected error on tampered webhook payload")
	}

	// 2. Incoming Hook HMAC-SHA256 (with and without prefix)
	sig := SignHookPayload(v.IncomingHook.Body, v.IncomingHook.Secret)
	if sig != v.IncomingHook.PrefixedSignature {
		t.Fatalf("SignHookPayload = %q, want %q", sig, v.IncomingHook.PrefixedSignature)
	}
	if !VerifyHookSignature(v.IncomingHook.Body, sig, v.IncomingHook.Secret) {
		t.Fatal("VerifyHookSignature returned false for prefixed signature")
	}
	if !VerifyHookSignature(v.IncomingHook.Body, v.IncomingHook.HexSignature, v.IncomingHook.Secret) {
		t.Fatal("VerifyHookSignature returned false for raw hex signature")
	}

	// 3. Money (Tiyn <-> Tenge + FormatTiyn) & Kazakhstan BIN/IIN
	for _, mc := range v.Money {
		if got := ToTiyn(mc.Tenge); got != mc.Tiyn {
			t.Fatalf("ToTiyn(%v) = %d, want %d", mc.Tenge, got, mc.Tiyn)
		}
		if got := ToTenge(mc.Tiyn); got != mc.Tenge {
			t.Fatalf("ToTenge(%d) = %v, want %v", mc.Tiyn, got, mc.Tenge)
		}
		if got := FormatTiyn(mc.Tiyn, "KZT"); got != mc.Formatted {
			t.Fatalf("FormatTiyn(%d) = %q, want %q", mc.Tiyn, got, mc.Formatted)
		}
	}
	for _, bc := range v.BinIin {
		if got := IsValidBinIin(bc.Value); got != bc.Valid {
			t.Fatalf("IsValidBinIin(%q) = %v, want %v", bc.Value, got, bc.Valid)
		}
	}

	// 4. Structured API Errors + Hint
	for _, ec := range v.Errors {
		apiErr := ParseAPIError(ec.Status, []byte(ec.Body), nil)
		if apiErr.Code != ec.ErrorCode || apiErr.RequestID != ec.RequestID || apiErr.Hint != ec.Hint {
			t.Fatalf("ParseAPIError(%d) = %+v", ec.Status, apiErr)
		}
		if ec.Status == 429 && !IsRateLimitError(apiErr) {
			t.Fatal("expected IsRateLimitError(429) == true")
		}
		if ec.Status == 402 && !IsPaymentRequiredError(apiErr) {
			t.Fatal("expected IsPaymentRequiredError(402) == true")
		}
	}

	// 5. SSE Stream Parser & AccumulateChatStream
	var deltas []string
	acc, err := AccumulateChatStream(ParseSSE(strings.NewReader(v.SSE.RawStream)), func(d string) {
		deltas = append(deltas, d)
	})
	if err != nil {
		t.Fatalf("AccumulateChatStream error: %v", err)
	}
	if len(acc.Events) != len(v.SSE.Events) {
		t.Fatalf("got %d SSE events, want %d", len(acc.Events), len(v.SSE.Events))
	}
	if acc.Text != v.SSE.ExpectedText || strings.Join(deltas, "") != v.SSE.ExpectedText {
		t.Fatalf("acc.Text = %q, want %q", acc.Text, v.SSE.ExpectedText)
	}
	if acc.DialogID != "dlg_01" || acc.CostTiyn != 185 {
		t.Fatalf("unexpected acc metadata: %+v", acc)
	}
}

func TestClientAskRetryAndPagination(t *testing.T) {
	var attempts atomic.Int32
	var seenIdemKey string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("x-ratelimit-limit", "600")
		w.Header().Set("x-ratelimit-remaining", "598")
		w.Header().Set("x-ratelimit-reset", "1762167600")

		switch r.URL.Path {
		case "/v1/companies/co_demo/chat":
			n := attempts.Add(1)
			idem := r.Header.Get("Idempotency-Key")
			if idem == "" {
				t.Errorf("missing Idempotency-Key header")
			}
			if seenIdemKey == "" {
				seenIdemKey = idem
			} else if seenIdemKey != idem {
				t.Errorf("Idempotency-Key changed across retry: %q vs %q", seenIdemKey, idem)
			}
			if n == 1 {
				w.Header().Set("Retry-After", "0.01")
				w.WriteHeader(http.StatusTooManyRequests)
				_, _ = w.Write([]byte(`{"error":{"code":"rate_limit_exceeded","message":"Slow down"}}`))
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"dialog_id":"conv_1","answer":"Сальдо: 45 000,00 ₸","turn_no":1,"proposals":[],"questions":[],"citations":[],"errors":[]}`))

		case "/v1/companies/co_demo/proposals":
			cur := r.URL.Query().Get("cursor")
			w.Header().Set("Content-Type", "application/json")
			if cur == "" {
				_, _ = w.Write([]byte(`{"data":[{"id":"prop_1"},{"id":"prop_2"}],"next_cursor":"cur_2"}`))
			} else {
				_, _ = w.Write([]byte(`{"data":[{"id":"prop_3"}],"next_cursor":null}`))
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	client := NewClient(
		WithAPIKey("tg_live_test"),
		WithBaseURL(srv.URL),
		WithCompanyID("co_demo"),
		WithMaxRetries(2),
	)

	ans, err := client.Ask(context.Background(), AskInput{Prompt: "Какое сальдо?"})
	if err != nil {
		t.Fatalf("Ask error: %v", err)
	}
	if ans.Reply != "Сальдо: 45 000,00 ₸" || ans.DialogID != "conv_1" {
		t.Fatalf("unexpected AskOutput: %+v", ans)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected 2 attempts (1 retry), got %d", attempts.Load())
	}
	if rl := client.LastRateLimit(); rl == nil || rl.Remaining != 598 {
		t.Fatalf("unexpected LastRateLimit: %+v", rl)
	}

	items, err := client.Proposals.ListAll(context.Background(), nil)
	if err != nil {
		t.Fatalf("Proposals.ListAll error: %v", err)
	}
	if len(items) != 3 || items[2].ID != "prop_3" {
		t.Fatalf("expected 3 proposals across 2 pages, got %+v", items)
	}
}
