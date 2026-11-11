package tungo_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	tungo "github.com/TungoAI/tungo-sdk-go"
)

// The functions named readme* are the code of the README, word for word: the generator cuts
// the text between the "README:" markers into the README, and TestReadmeSnippets runs every one
// of them against a local mock, so the README cannot drift from the SDK.

func readmeConfig(ctx context.Context) error {
	// README:config
	client := tungo.NewClient(
		tungo.WithAPIKey(os.Getenv("MY_TUNGO_KEY")),  // default (also when the value is empty): TUNGO_API_KEY
		tungo.WithCompanyID("co_123"),                // default: TUNGO_COMPANY_ID
		tungo.WithWorkspaceID("ws_123"),              // default: TUNGO_WORKSPACE_ID
		tungo.WithBaseURL(os.Getenv("MY_TUNGO_URL")), // default (also when the value is empty): TUNGO_BASE_URL, then https://api.tungo.kz
		tungo.WithTimeout(30*time.Second),            // default: 60 s, 600 s for a chat turn
		tungo.WithMaxRetries(3),                      // default: 2
		tungo.WithHTTPClient(&http.Client{}),         // default: a shared client with keep-alive
		tungo.WithHeader("X-Trace", "billing-job"),
	)

	// Every option can also be passed to one call. The client itself never changes, so it is
	// safe to share between goroutines.
	var resp *tungo.Response
	dialogs, err := client.Chat.List(ctx, &tungo.ChatListParams{Limit: tungo.Ptr(int64(20))},
		tungo.WithCompanyID("co_456"),
		tungo.WithResponseInto(&resp),
	)
	if err != nil {
		return err
	}
	fmt.Println(len(dialogs.Data), "dialogs, request id", resp.RequestID())

	// A client for another company that shares the connections of the first one.
	other := client.WithOptions(tungo.WithCompanyID("co_789"))
	// README:end
	_ = other
	return nil
}

func readmeErrors(ctx context.Context, client *tungo.Client) error {
	// README:errors
	_, err := client.Proposals.Get(ctx, "P-000042")

	var apiErr *tungo.APIError
	var timeoutErr *tungo.TimeoutError
	var connErr *tungo.ConnectionError
	switch {
	case err == nil:
		// success
	case errors.As(err, &apiErr):
		// The API answered with an error. Kind is the stable thing to branch on.
		switch apiErr.Kind {
		case tungo.KindNotFound:
			fmt.Println("no such proposal")
		case tungo.KindLimit:
			if apiErr.HasRetryAfter {
				fmt.Println("limit reached, try again in", apiErr.RetryAfter)
			}
		default:
			fmt.Println(apiErr.StatusCode, apiErr.Code, apiErr.Message, "request id:", apiErr.RequestID)
		}
	case errors.As(err, &timeoutErr):
		fmt.Println("no answer within", timeoutErr.Limit)
	case errors.As(err, &connErr):
		fmt.Println("could not reach the API:", connErr)
	case errors.Is(err, context.Canceled):
		fmt.Println("cancelled by the caller")
	default:
		return err // *tungo.DecodeError, *tungo.ConfigError, *tungo.ValidationError, ...
	}

	// When only the kind matters:
	if tungo.KindOf(err) == tungo.KindAuthentication {
		fmt.Println("check TUNGO_API_KEY")
	}
	// README:end
	return nil
}

func readmeRetries(ctx context.Context, base *tungo.Client) error {
	// README:retries
	// For the whole client...
	client := base.WithOptions(
		tungo.WithMaxRetries(5),           // 0 disables retries
		tungo.WithTimeout(20*time.Second), // for every attempt, the response body included
	)

	// ...or for one call.
	account, err := client.Platform.GetAccount(ctx,
		tungo.WithMaxRetries(0),
		tungo.WithRequestTimeout(5*time.Second),
	)
	if err != nil {
		return err
	}
	fmt.Println(account.BalanceTiyn)

	// The deadline of the context bounds the call as a whole, retries and pauses included.
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	// A POST is retried only with an Idempotency-Key. The SDK generates one for the operations
	// that accept it and repeats it on every attempt; pass your own to make a retry of your
	// whole job safe as well.
	turn, err := client.Chat.Create(ctx,
		tungo.ChatMessageRequest{Message: tungo.Ptr("Проведи выписку за вчера")},
		tungo.WithIdempotencyKey("statement-2026-11-09"),
	)
	if err != nil {
		return err
	}
	fmt.Println(turn.Answer)
	// README:end
	return nil
}

func readmePagination(ctx context.Context, client *tungo.Client) error {
	// README:pagination
	params := &tungo.ProposalsListParams{Status: tungo.Ptr("draft"), Limit: tungo.Ptr(int64(50))}

	// One page at a time.
	page, err := client.Proposals.List(ctx, params)
	if err != nil {
		return err
	}
	for _, proposal := range page.Data {
		fmt.Println(proposal.ID, proposal.Total.Formatted)
	}
	if page.HasNext() {
		if page, err = page.Next(ctx); err != nil { // same filters, next cursor
			return err
		}
		fmt.Println(len(page.Data), "more")
	}

	// Or every item of every page. Pages are fetched as the loop needs them; break to stop.
	for proposal, err := range client.Proposals.ListAll(ctx, params) {
		if err != nil {
			return err
		}
		fmt.Println(proposal.ID)
	}
	// README:end
	return nil
}

func readmeStreaming(ctx context.Context, client *tungo.Client) error {
	// README:streaming
	stream, err := client.Chat.CreateStream(ctx, tungo.ChatMessageRequest{
		Message: tungo.Ptr("Что с НДС за квартал?"),
	})
	if err != nil {
		return err // a refused request is the same *tungo.APIError as for any other call
	}
	defer stream.Close() // not needed after a complete loop, harmless and safe on early return

	for event, err := range stream.Events() {
		if err != nil {
			return err
		}
		switch e := event.Value.(type) {
		case *tungo.ChatEventText:
			fmt.Print(e.Delta)
		case *tungo.ChatEventTool:
			fmt.Printf("\n[%s: %s]\n", e.Name, e.Status)
		case *tungo.ChatEventDone:
			fmt.Println()
		case *tungo.UnknownVariant:
			// an event type added to the API after this SDK version: e.Tag, e.Raw
		}
	}
	fmt.Println("dialog:", stream.DialogID())

	// When the pieces are not needed, let the SDK put the turn together.
	stream, err = client.Chat.CreateStream(ctx, tungo.ChatMessageRequest{
		Message:  tungo.Ptr("А за прошлый?"),
		DialogID: tungo.Value(stream.DialogID()),
	})
	if err != nil {
		return err
	}
	turn, err := stream.Accumulate(nil)
	if err != nil {
		return err // *tungo.TurnError when the turn itself failed
	}
	fmt.Println(turn.Text, turn.StopReason)
	// README:end
	return nil
}

func readmeWebhooks(ctx context.Context, client *tungo.Client, w http.ResponseWriter, r *http.Request) error {
	// README:webhooks
	// Receiving: verify the bytes exactly as they arrived, before decoding anything.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	// The secret (whsec_...) comes from WithWebhookSecret or TUNGO_WEBHOOK_SECRET. Messages
	// older or newer than five minutes are rejected; an empty secret is always an error.
	event, err := client.VerifyWebhook(body, r.Header)
	if err != nil {
		var verr *tungo.WebhookVerificationError
		if errors.As(err, &verr) {
			log.Println("rejected a webhook:", verr.Reason)
		}
		w.WriteHeader(http.StatusBadRequest)
		return nil
	}
	switch e := event.Value.(type) {
	case *tungo.WebhookEventProposalWritten:
		fmt.Println("written to 1C:", e.Data.ProposalID)
	default:
		fmt.Println("event", event.Tag())
	}
	w.WriteHeader(http.StatusNoContent)

	// Sending: start an automation through its hook. No API key is sent; the payload is signed
	// with the hook secret, and the event id makes a repeated delivery harmless.
	accepted, err := client.SendHook(ctx, "hk_token",
		map[string]any{"order_id": "A-17", "amount_tiyn": 1250000},
		&tungo.SendHookParams{Secret: os.Getenv("TUNGO_HOOK_SECRET"), EventID: "order-A-17"},
	)
	if err != nil {
		return err
	}
	fmt.Println(accepted.EventID, accepted.Duplicate)
	// README:end
	return nil
}

func readmeMoney() error {
	// README:money
	// The API counts money in tiyn (1 ₸ = 100 tiyn) as int64. Convert from decimal strings:
	// they are exact, floats are not.
	tiyn, err := tungo.ToTiyn("1234.50")
	if err != nil {
		return err
	}
	fmt.Println(tiyn)                          // 123450
	fmt.Println(tungo.FormatTiyn(tiyn, "KZT")) // 1 234,50 ₸
	fmt.Println(tungo.TiynToDecimal(tiyn))     // 1234.50
	fmt.Println(tungo.IsValidBINIIN("x"))      // false
	half, _ := tungo.ToTiynFloat(0.285)        // floats are accepted and rounded half away from zero
	fmt.Println(half)                          // 29
	// README:end
	return nil
}

func readmeNulls(ctx context.Context, client *tungo.Client) error {
	// README:nulls
	// Optional fields are pointers: nil is "not sent", tungo.Ptr(false) is false.
	// Fields the API accepts null for are tungo.Nullable: the zero value is "not sent".
	_, err := client.Webhooks.UpdateEndpoint(ctx, "ep_1", tungo.WebhookEndpointPatch{
		Enabled:    tungo.Ptr(false),        // "enabled": false
		EventTypes: tungo.Value([]string{}), // "event_types": []
		Channels:   tungo.Null[[]string](),  // "channels": null
		// Description is not set: the property is not sent and stays as it is.
	})
	if err != nil {
		return err
	}

	// Responses keep what this SDK version does not know about.
	endpoint, err := client.Webhooks.GetEndpoint(ctx, "ep_1")
	if err != nil {
		return err
	}
	fmt.Println(endpoint.ExtraFields()) // properties added to the API later, as raw JSON
	// README:end
	return nil
}

func readmeWaiting(ctx context.Context, client *tungo.Client) error {
	// README:waiting
	// Start a scenario and wait until its run reaches a final state.
	run, err := client.RunScenarioAndWait(ctx, "scn_1", nil, &tungo.WaitOptions{Timeout: 10 * time.Minute})
	if err != nil {
		return err // *tungo.TimeoutError when the run is still going after ten minutes
	}
	fmt.Println(run.State)

	// Upload a bank statement and wait for the batch that processes it.
	statement := []byte("1CClientBankExchange\n...")
	job, err := client.UploadBankStatement(ctx, "statement.txt", statement, true)
	if err != nil {
		return err
	}
	if job, err = client.WaitForBatch(ctx, job.ID, nil); err != nil {
		return err
	}
	fmt.Println(job.Running) // false
	// README:end
	return nil
}

// readmeMock answers every request of the README snippets.
func readmeMock(t *testing.T) *httptest.Server {
	const endpoint = `{"id":"ep_1","url":"https://example.com/hook","enabled":false,"event_types":[],"channels":null,"description":"","created_at":"2026-11-09T10:00:00Z","updated_at":"2026-11-09T10:00:00Z","later_field":1}`
	const run = `{"id":"run_1","scenario_id":"scn_1","state":"done"}`
	money := func(tiyn int) string {
		return `{"amount":` + strconv.Itoa(tiyn/100) + `,"amount_tiyn":` + strconv.Itoa(tiyn) + `,"formatted":"` + tungo.FormatTiyn(int64(tiyn), "KZT") + `"}`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		path := r.Method + " " + r.URL.Path
		switch {
		case strings.HasSuffix(path, "/chat") && r.Method == "GET":
			w.Header().Set("x-request-id", "req_1")
			_, _ = io.WriteString(w, `{"data":[],"next_cursor":null}`)
		case strings.HasSuffix(path, "/chat") && r.Header.Get("Accept") == "text/event-stream":
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("X-Dialog-Id", "dlg_1")
			_, _ = io.WriteString(w, "id: 1\nevent: text\ndata: {\"type\":\"text\",\"delta\":\"НДС к уплате: \"}\n\n"+
				"id: 2\nevent: tool\ndata: {\"type\":\"tool\",\"name\":\"vat_report\",\"status\":\"done\"}\n\n"+
				"id: 3\nevent: text\ndata: {\"type\":\"text\",\"delta\":\"120 000 ₸\"}\n\n"+
				"id: 4\nevent: done\ndata: {\"type\":\"done\",\"stop_reason\":\"end_turn\"}\n\n")
		case strings.HasSuffix(path, "/chat"):
			_, _ = io.WriteString(w, `{"dialog_id":"dlg_1","turn_no":1,"answer":"Готово","proposals":[],"pending_question":null,"questions":[],"citations":[],"errors":[]}`)
		case strings.HasSuffix(path, "/proposals/P-000042"):
			w.WriteHeader(404)
			_, _ = io.WriteString(w, `{"type":"error","error":{"type":"not_found_error","message":"Предложение не найдено"},"request_id":"req_2"}`)
		case strings.HasSuffix(path, "/proposals"):
			next := `"c2"`
			if r.URL.Query().Get("cursor") != "" {
				next = "null"
			}
			_, _ = io.WriteString(w, `{"data":[{"id":"P-`+r.URL.Query().Get("cursor")+`1","total":`+money(123450)+`,"vat":`+money(0)+`}],"next_cursor":`+next+`,"counts":{},"total":2}`)
		case strings.HasSuffix(path, "/account"):
			_, _ = io.WriteString(w, `{"balance_tiyn":500000}`)
		case strings.HasPrefix(path, "POST /hooks/"):
			w.WriteHeader(202)
			_, _ = io.WriteString(w, `{"accepted":true,"event_id":"hev_1","duplicate":false,"run_ids":[]}`)
		case strings.Contains(path, "/webhooks/endpoints/"):
			_, _ = io.WriteString(w, endpoint)
		case strings.HasSuffix(path, "/run"), strings.Contains(path, "/automations/runs/"):
			_, _ = io.WriteString(w, run)
		case strings.Contains(path, "/batch"), strings.Contains(path, "/bank"):
			_, _ = io.WriteString(w, `{"id":"bat_1","kind":"bank_statement","title":"t","started_at":"2026-11-09T10:00:00Z","running":false,"events":1}`)
		default:
			t.Errorf("the README mock has no answer for %s", path)
			w.WriteHeader(500)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// signedWebhook builds a webhook request signed the way Tungo signs them.
func signedWebhook(secret string, at time.Time, body string) *http.Request {
	key, _ := base64.StdEncoding.DecodeString(strings.TrimPrefix(secret, "whsec_"))
	ts := strconv.FormatInt(at.Unix(), 10)
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("msg_1." + ts + "." + body))
	r := httptest.NewRequest("POST", "/webhooks/tungo", strings.NewReader(body))
	r.Header.Set("webhook-id", "msg_1")
	r.Header.Set("webhook-timestamp", ts)
	r.Header.Set("webhook-signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	return r
}

const exampleSecret = "whsec_MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY="

const exampleEvent = `{"id":"evt_1","type":"proposal.written","created_at":"2026-11-09T15:00:00.000Z","workspace_id":"ws_1","company_id":"co_1","data":{"proposal_id":"P-000042","kind":"payment_out","status":"written","amount_tiyn":1250000,"onec_document_number":"00-17"}}`

// TestReadmeSnippets runs the code of every README section against a local mock.
func TestReadmeSnippets(t *testing.T) {
	srv := readmeMock(t)
	t.Setenv("MY_TUNGO_KEY", "tg_test")
	t.Setenv("MY_TUNGO_URL", srv.URL)
	t.Setenv("TUNGO_HOOK_SECRET", "hksec_test")
	ctx := context.Background()
	client := tungo.NewClient(tungo.WithAPIKey("tg_test"), tungo.WithBaseURL(srv.URL),
		tungo.WithCompanyID("co_1"), tungo.WithWorkspaceID("ws_1"), tungo.WithWebhookSecret(exampleSecret))

	// The snippets print; keep the test output quiet.
	stdout := os.Stdout
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = devNull
	defer func() {
		os.Stdout = stdout
		devNull.Close()
	}()

	recorder := httptest.NewRecorder()
	snippets := map[string]func() error{
		"config":     func() error { return readmeConfig(ctx) },
		"errors":     func() error { return readmeErrors(ctx, client) },
		"retries":    func() error { return readmeRetries(ctx, client) },
		"pagination": func() error { return readmePagination(ctx, client) },
		"streaming":  func() error { return readmeStreaming(ctx, client) },
		"webhooks": func() error {
			return readmeWebhooks(ctx, client, recorder, signedWebhook(exampleSecret, time.Now(), exampleEvent))
		},
		"money":   readmeMoney,
		"nulls":   func() error { return readmeNulls(ctx, client) },
		"waiting": func() error { return readmeWaiting(ctx, client) },
	}
	for name, run := range snippets {
		if err := run(); err != nil {
			t.Errorf("README section %q: %v", name, err)
		}
	}
	if recorder.Code != http.StatusNoContent {
		t.Errorf("the webhook snippet answered %d to a valid webhook", recorder.Code)
	}
}

// ---- runnable examples (shown in the package documentation) -----------------------------------

func ExampleToTiyn() {
	tiyn, err := tungo.ToTiyn("1234.5")
	fmt.Println(tiyn, err)

	_, err = tungo.ToTiyn("12,50")
	fmt.Println(err != nil)
	// Output:
	// 123450 <nil>
	// true
}

func ExampleFormatTiyn() {
	fmt.Println(tungo.FormatTiyn(123450, "KZT"))
	fmt.Println(tungo.TiynToDecimal(-5))
	// Output:
	// 1 234,50 ₸
	// -0.05
}

func ExampleVerifyWebhook() {
	sentAt := time.Date(2026, 11, 9, 15, 0, 0, 0, time.UTC)
	request := signedWebhook(exampleSecret, sentAt, exampleEvent)
	body, _ := io.ReadAll(request.Body)

	// The clock is fixed here only to make the example reproducible; leave it out in real code.
	clock := tungo.WithWebhookClock(func() time.Time { return sentAt.Add(time.Minute) })

	event, err := tungo.VerifyWebhook(body, request.Header, exampleSecret, clock)
	if err != nil {
		fmt.Println("rejected:", err)
		return
	}
	if written, ok := event.Value.(*tungo.WebhookEventProposalWritten); ok {
		fmt.Println(event.Tag(), written.Data.ProposalID, *written.Data.AmountTiyn)
	}

	// The same bytes with another secret are rejected.
	_, err = tungo.VerifyWebhook(body, request.Header, "whsec_b3RoZXI=", clock)
	var verr *tungo.WebhookVerificationError
	fmt.Println(errors.As(err, &verr), verr.Reason)
	// Output:
	// proposal.written P-000042 1250000
	// true signature_mismatch
}

func ExampleClient_Ask() {
	// A stand-in for the API, so that the example runs anywhere.
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"dialog_id":"dlg_1","turn_no":1,"answer":"На счёте 45 000,00 ₸","proposals":[],"pending_question":null,"questions":[],"citations":[],"errors":[]}`)
	}))
	defer api.Close()

	client := tungo.NewClient(tungo.WithAPIKey("tg_test"), tungo.WithCompanyID("co_1"), tungo.WithBaseURL(api.URL))
	turn, err := client.Ask(context.Background(), "Сколько денег на счёте?")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(turn.Answer)
	// Output:
	// На счёте 45 000,00 ₸
}

func ExampleChatStream() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: text\ndata: {\"type\":\"text\",\"delta\":\"Привет, \"}\n\n"+
			"event: text\ndata: {\"type\":\"text\",\"delta\":\"мир\"}\n\n"+
			"event: done\ndata: {\"type\":\"done\",\"stop_reason\":\"end_turn\"}\n\n")
	}))
	defer api.Close()

	client := tungo.NewClient(tungo.WithAPIKey("tg_test"), tungo.WithCompanyID("co_1"), tungo.WithBaseURL(api.URL))
	stream, err := client.Chat.CreateStream(context.Background(), tungo.ChatMessageRequest{Message: tungo.Ptr("Привет")})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer stream.Close()
	for event, err := range stream.Events() {
		if err != nil {
			fmt.Println(err)
			return
		}
		if text, ok := event.Value.(*tungo.ChatEventText); ok {
			fmt.Print(text.Delta)
		}
	}
	fmt.Println()
	// Output:
	// Привет, мир
}

func ExampleProposalsService_ListAll() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("cursor") == "" {
			_, _ = io.WriteString(w, `{"data":[{"id":"P-1"},{"id":"P-2"}],"next_cursor":"next","counts":{},"total":3}`)
			return
		}
		_, _ = io.WriteString(w, `{"data":[{"id":"P-3"}],"next_cursor":null,"counts":{},"total":3}`)
	}))
	defer api.Close()

	client := tungo.NewClient(tungo.WithAPIKey("tg_test"), tungo.WithCompanyID("co_1"), tungo.WithBaseURL(api.URL))
	for proposal, err := range client.Proposals.ListAll(context.Background(), nil) {
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(proposal.ID)
	}
	// Output:
	// P-1
	// P-2
	// P-3
}

func ExampleAPIError() {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = io.WriteString(w, `{"type":"error","error":{"type":"not_found_error","message":"Предложение не найдено"},"request_id":"req_42"}`)
	}))
	defer api.Close()

	client := tungo.NewClient(tungo.WithAPIKey("tg_test"), tungo.WithCompanyID("co_1"), tungo.WithBaseURL(api.URL))
	_, err := client.Proposals.Get(context.Background(), "P-404")

	var apiErr *tungo.APIError
	if errors.As(err, &apiErr) {
		fmt.Println(apiErr.StatusCode, apiErr.Kind, apiErr.RequestID)
	}
	fmt.Println(err)
	// Output:
	// 404 not_found_error req_42
	// tungo: API error 404 not_found_error: Предложение не найдено [request id req_42]
}

func ExampleNullable() {
	patch := tungo.WebhookEndpointPatch{
		Enabled:    tungo.Ptr(false),
		EventTypes: tungo.Value([]string{}),
		Channels:   tungo.Null[[]string](),
	}
	body, _ := json.Marshal(patch)
	fmt.Println(string(body))
	// Output:
	// {"channels":null,"enabled":false,"event_types":[]}
}

func ExampleClient_Format() {
	client := tungo.NewClient(tungo.WithAPIKey("tg_live_secret"), tungo.WithCompanyID("co_1"), tungo.WithBaseURL("https://api.tungo.kz"), tungo.WithWorkspaceID(""))
	fmt.Printf("%+v\n", client)
	// Output:
	// tungo.Client{BaseURL: "https://api.tungo.kz", APIKey: [REDACTED], CompanyID: "co_1", WorkspaceID: ""}
}
