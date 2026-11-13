package tungo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
)

// Ask sends one message to the AI accountant and waits for the whole answer (Chat.Create with
// stream:false). The answer is in the Answer field of the result, the dialog id in DialogID:
// to continue the conversation call Chat.Create with that dialog id.
//
// When the turn itself failed (the result lists errors), Ask returns the result together with a
// *TurnError for the first of them. Use Chat.CreateStream to receive the answer as it is produced.
func (c *Client) Ask(ctx context.Context, message string, opts ...Option) (*ChatTurnResult, error) {
	res, err := c.Chat.Create(ctx, ChatMessageRequest{Message: &message}, opts...)
	if err != nil {
		return nil, err
	}
	if len(res.Errors) > 0 {
		return res, newTurnError(res.Errors[0], "")
	}
	return res, nil
}

// SendHookParams are the optional settings of Client.SendHook.
type SendHookParams struct {
	// Secret is the signing secret of the hook. When it is set, the request carries
	// X-Signature: sha256=<hex HMAC-SHA256 of the exact bytes sent>.
	Secret string
	// EventID is the sender's id of the event (X-Event-Id): the API accepts an event with the
	// same id only once.
	EventID string
	// ContentType is the Content-Type of the body; the default is application/json.
	ContentType string
}

// SendHook posts an event to an incoming hook of a scenario (POST /hooks/{token}). The token in
// the path authenticates the request: the API key is never sent.
//
// payload is sent as it is when it is a []byte, a string or a json.RawMessage, and is encoded
// as JSON otherwise. SendHook is Automations.PostHook plus the encoding and the signature, so
// both behave identically (same retries, same Idempotency-Key handling).
func (c *Client) SendHook(ctx context.Context, token string, payload any, params *SendHookParams, opts ...Option) (*AutomationHookAccepted, error) {
	var body []byte
	switch v := payload.(type) {
	case []byte:
		body = v
	case json.RawMessage:
		body = v
	case string:
		body = []byte(v)
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil, &ValidationError{Param: "payload", Message: "the hook payload cannot be encoded as JSON: " + err.Error(), Err: err}
		}
		body = encoded
	}
	var p AutomationsPostHookParams
	if params != nil {
		p.ContentType = params.ContentType
		if params.EventID != "" {
			p.EventID = &params.EventID
		}
		if params.Secret != "" {
			signature, err := SignHook(body, params.Secret)
			if err != nil {
				return nil, err
			}
			p.Signature = &signature
		}
	}
	return c.Automations.PostHook(ctx, token, body, &p, opts...)
}

// UploadBankStatement uploads a bank statement for processing (Batches.CreateBankStatement).
// With isText, UTF-8 content (a 1CClientBankExchange file already decoded) is sent as text; any
// other file, and text that is not valid UTF-8 (a bank export in cp1251 or cp866 as read from
// disk), is sent base64-encoded as it is, and the API decodes it by its Кодировка= header. Wait
// for the result with WaitForBatch.
func (c *Client) UploadBankStatement(ctx context.Context, fileName string, content []byte, isText bool, opts ...Option) (*BatchJob, error) {
	req := BankStatementJobRequest{FileName: &fileName}
	// encoding/json would replace every byte that is not UTF-8 with U+FFFD, silently.
	if isText && utf8.Valid(content) {
		req.Text = Ptr(string(content))
	} else {
		req.File = Ptr(base64.StdEncoding.EncodeToString(content))
	}
	return c.Batches.CreateBankStatement(ctx, req, opts...)
}

// WaitOptions tunes the polling helpers. The zero value (or nil) uses the defaults.
type WaitOptions struct {
	// Timeout is the overall deadline of the wait; the default is 10 minutes. When it passes, the
	// helper returns a *TimeoutError (a local error, not an HTTP status).
	Timeout time.Duration
	// PollInterval is the pause before the second poll; the default is 1 s. Each following pause
	// is 1.5 times longer, up to MaxPollInterval.
	PollInterval time.Duration
	// MaxPollInterval caps the pause between two polls; the default is 5 s.
	MaxPollInterval time.Duration
}

func (w *WaitOptions) resolved() (timeout, interval, maxInterval time.Duration) {
	timeout, interval, maxInterval = 10*time.Minute, time.Second, 5*time.Second
	if w == nil {
		return
	}
	if w.Timeout > 0 {
		timeout = w.Timeout
	}
	if w.PollInterval > 0 {
		interval = w.PollInterval
	}
	if w.MaxPollInterval > 0 {
		maxInterval = w.MaxPollInterval
	}
	if interval > maxInterval {
		interval = maxInterval
	}
	return
}

// transientPollError reports whether a failed poll is worth repeating: the same class of
// failures the SDK retries by itself (connection errors, timeouts of one attempt, 408, 429 and
// 5xx answers). Anything else (404, 401, a response that does not decode) ends the wait.
func transientPollError(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return retryableStatus(apiErr.StatusCode)
	}
	return isConnectionOrTimeout(err)
}

// poll calls check until it reports done, a permanent error occurs, the deadline passes or ctx
// is cancelled. A transient failure of one poll is tolerated: see transientPollError.
func poll(ctx context.Context, cfg config, what string, w *WaitOptions, check func(ctx context.Context) (done bool, err error)) error {
	timeout, interval, maxInterval := w.resolved()
	deadline := errors.New("the wait deadline elapsed")
	waitCtx, cancel := context.WithTimeoutCause(ctx, timeout, deadline)
	defer cancel()
	expired := func() error {
		if ctx.Err() != nil {
			return contextError(ctx, what, ctx.Err())
		}
		return &TimeoutError{Op: what, Limit: timeout, Err: fmt.Errorf("%w: %w", context.DeadlineExceeded, deadline)}
	}
	for {
		done, err := check(waitCtx)
		if err != nil {
			if waitCtx.Err() != nil {
				return expired()
			}
			if !transientPollError(err) {
				return err
			}
			// A poll that failed for a passing reason (after its own retries) does not say
			// anything about the job: keep waiting until the deadline.
		}
		if done {
			return nil
		}
		if cfg.pause(waitCtx, interval) != nil {
			return expired()
		}
		interval = min(interval*3/2, maxInterval)
	}
}

// runFinished reports whether the state of a run is terminal according to the API contract.
// queued, running and any state this SDK version does not know are not: the run is polled again.
func runFinished(state AutomationRunState) bool {
	switch state {
	case AutomationRunStateDone, AutomationRunStateError, AutomationRunStateSkipped, AutomationRunStateCancelled:
		return true
	}
	return false
}

// WaitForRun polls a scenario run (Automations.GetRun) until its state is terminal: done, error,
// skipped or cancelled. The pause between polls grows from 1 s to 5 s. It returns a
// *TimeoutError when the deadline of w passes first, and honours the cancellation of ctx.
func (c *Client) WaitForRun(ctx context.Context, runID string, w *WaitOptions, opts ...Option) (*AutomationRunDetail, error) {
	var run *AutomationRunDetail
	err := poll(ctx, c.cfg.with(opts), "wait for run "+runID, w, func(ctx context.Context) (bool, error) {
		res, err := c.Automations.GetRun(ctx, runID, opts...)
		if err != nil {
			return false, err
		}
		if res.State == "" {
			return false, &DecodeError{Op: "wait for run " + runID, StatusCode: 200, Body: []byte(res.RawJSON()), Err: errors.New("the run has no state")}
		}
		run = res
		return runFinished(AutomationRunState(res.State)), nil
	})
	if err != nil {
		return nil, err
	}
	return run, nil
}

// RunScenarioAndWait starts a scenario (Automations.RunScenario) and waits for the run to finish
// with WaitForRun. body may be nil.
func (c *Client) RunScenarioAndWait(ctx context.Context, scenarioID string, body *AutomationScenarioRunInput, w *WaitOptions, opts ...Option) (*AutomationRunDetail, error) {
	started, err := c.Automations.RunScenario(ctx, scenarioID, body, opts...)
	if err != nil {
		return nil, err
	}
	// The idempotency key belongs to the start request only.
	return c.WaitForRun(ctx, started.ID, w, append(opts[:len(opts):len(opts)], WithIdempotencyKey(""))...)
}

// WaitForBatch polls a batch job (Batches.Get) until it is no longer running. A response without
// the running flag is not taken for "finished": it is a *DecodeError. It returns a
// *TimeoutError when the deadline of w passes first, and honours the cancellation of ctx.
func (c *Client) WaitForBatch(ctx context.Context, batchID string, w *WaitOptions, opts ...Option) (*BatchJob, error) {
	var job *BatchJob
	err := poll(ctx, c.cfg.with(opts), "wait for batch "+batchID, w, func(ctx context.Context) (bool, error) {
		res, err := c.Batches.Get(ctx, batchID, opts...)
		if err != nil {
			return false, err
		}
		var probe struct {
			Running *bool `json:"running"`
		}
		if json.Unmarshal([]byte(res.RawJSON()), &probe) != nil || probe.Running == nil {
			return false, &DecodeError{Op: "wait for batch " + batchID, StatusCode: 200, Body: []byte(res.RawJSON()), Err: errors.New("the batch has no running flag")}
		}
		job = res
		return !*probe.Running, nil
	})
	if err != nil {
		return nil, err
	}
	return job, nil
}
