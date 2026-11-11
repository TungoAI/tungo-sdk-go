package tungo

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"iter"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// SSEEvent is one server-sent event as defined by the WHATWG HTML standard.
type SSEEvent struct {
	// Event is the event name; "message" when the stream did not name the event.
	Event string
	// Data is the payload: the data lines joined with "\n".
	Data string
	// ID is the last event id seen so far ("" when the stream sent none).
	ID string
	// Retry is the reconnection time in milliseconds when this event's block set one, else nil.
	Retry *int
}

// sseParser is an incremental parser of the text/event-stream format (WHATWG HTML,
// "Interpreting an event stream"). It accepts LF, CRLF and bare CR line endings, strips a
// leading BOM, has no line-length limit and tolerates chunks that split a line ending or a
// UTF-8 sequence.
type sseParser struct {
	buf     []byte
	scanned int  // bytes of buf already known to hold no line end
	started bool // the BOM check is done
	data    []byte
	hasData bool
	event   string
	lastID  string
	retry   *int
	ready   []SSEEvent
}

var utf8BOM = []byte("\xef\xbb\xbf")

// feed consumes a chunk and queues the events it completes.
func (p *sseParser) feed(chunk []byte) {
	p.buf = append(p.buf, chunk...)
	if !p.started {
		if len(p.buf) < len(utf8BOM) && bytes.HasPrefix(utf8BOM, p.buf) {
			return // may still turn out to be a BOM
		}
		p.buf = bytes.TrimPrefix(p.buf, utf8BOM)
		p.started = true
	}
	// A long line arrives in many chunks: search only the bytes not searched before, so the
	// work stays linear in the size of the line.
	start, from := 0, p.scanned
	for {
		i := bytes.IndexAny(p.buf[from:], "\r\n")
		if i < 0 {
			break
		}
		i += from
		next := i + 1
		if p.buf[i] == '\r' {
			if i+1 == len(p.buf) {
				break // a CR at the end of a chunk may be the first half of CRLF
			}
			if p.buf[i+1] == '\n' {
				next = i + 2
			}
		}
		p.line(p.buf[start:i])
		start, from = next, next
	}
	// Keep only the unfinished line; copy it down so the buffer does not grow without bound.
	p.buf = append(p.buf[:0], p.buf[start:]...)
	p.scanned = len(p.buf)
	if n := len(p.buf); n > 0 && p.buf[n-1] == '\r' {
		p.scanned = n - 1 // the pending CR is looked at again
	}
}

// end is called when the stream closes: a trailing CR terminates its line, an event that was not
// terminated by a blank line is discarded.
func (p *sseParser) end() {
	if n := len(p.buf); n > 0 && p.buf[n-1] == '\r' {
		p.line(p.buf[:n-1])
	}
	p.buf = nil
}

func (p *sseParser) line(line []byte) {
	if len(line) == 0 {
		if p.hasData {
			name := p.event
			if name == "" {
				name = "message"
			}
			p.ready = append(p.ready, SSEEvent{Event: name, Data: string(p.data), ID: p.lastID, Retry: p.retry})
		}
		p.data, p.hasData, p.event, p.retry = nil, false, "", nil
		return
	}
	if line[0] == ':' {
		return // comment or keep-alive
	}
	field, value, _ := bytes.Cut(line, []byte(":"))
	value = bytes.TrimPrefix(value, []byte(" "))
	switch string(field) {
	case "event":
		p.event = string(value)
	case "data":
		if p.hasData {
			p.data = append(p.data, '\n')
		}
		p.data = append(p.data, value...)
		p.hasData = true
	case "id":
		if !bytes.Contains(value, []byte{0}) {
			p.lastID = string(value)
		}
	case "retry":
		if len(value) > 0 && strings.Trim(string(value), "0123456789") == "" {
			if n, err := strconv.Atoi(string(value)); err == nil {
				p.retry = &n
			}
		}
	}
}

// ParseSSE reads server-sent events from r until it ends, yielding each event as soon as it is
// complete. A read error other than io.EOF is yielded once, then the iteration stops. ParseSSE
// does not close r.
func ParseSSE(r io.Reader) iter.Seq2[SSEEvent, error] {
	return func(yield func(SSEEvent, error) bool) {
		var p sseParser
		chunk := make([]byte, 32*1024)
		for {
			n, err := r.Read(chunk)
			if n > 0 {
				p.feed(chunk[:n])
			}
			if err != nil {
				if !errors.Is(err, io.EOF) {
					for _, ev := range p.ready {
						if !yield(ev, nil) {
							return
						}
					}
					yield(SSEEvent{}, err)
					return
				}
				p.end()
			}
			for _, ev := range p.ready {
				if !yield(ev, nil) {
					return
				}
			}
			p.ready = p.ready[:0]
			if err != nil {
				return
			}
		}
	}
}

// Stream is an open server-sent event stream whose events decode into T.
//
// Iterate over Events exactly once. The connection is closed when the iteration ends for any
// reason (the stream finished, an error, or the loop was left early) and by Close. A stream
// that is never iterated must be closed, otherwise it keeps its connection until the context
// of the call is cancelled.
type Stream[T any] struct {
	call      *call
	closeOnce sync.Once
	mu        sync.Mutex
	lastID    string
	used      bool
	// tagProperty names the discriminator of T when the SSE event name carries the same value
	// ("type" for chat events); "" when T has none.
	tagProperty string
}

// streamCleanup releases the connection of a stream that was dropped without being closed.
func streamCleanup(cancel context.CancelCauseFunc) { cancel(nil) }

// openStream sends a streaming request and wraps the response.
func openStream[T any](ctx context.Context, c *Client, spec *requestSpec, opts []Option) (*Stream[T], error) {
	res, err := c.execute(ctx, spec, opts)
	if err != nil {
		return nil, err
	}
	s := &Stream[T]{call: res}
	runtime.AddCleanup(s, streamCleanup, res.cancel)
	return s, nil
}

// taggedEventData returns the data of an event ready for decoding. The API repeats the event
// name in the discriminator of the data; when the data is an object without it (a proxy or an
// older server that relies on the event name alone), the name is put in its place so that the
// event still decodes into its variant.
func taggedEventData(ev SSEEvent, property string) []byte {
	data := []byte(ev.Data)
	if property == "" || ev.Event == "" || ev.Event == "message" || jsonKind(data) != "object" {
		return data
	}
	var props map[string]json.RawMessage
	if json.Unmarshal(data, &props) != nil {
		return data
	}
	if _, ok := props[property]; ok {
		return data
	}
	tag, err := json.Marshal(ev.Event)
	if err != nil {
		return data
	}
	props[property] = tag
	if tagged, err := json.Marshal(props); err == nil {
		return tagged
	}
	return data
}

// Response returns the HTTP metadata of the stream response (status, headers, request id).
func (s *Stream[T]) Response() *Response { return s.call.response() }

// LastEventID returns the id of the last event received so far; send it in the Last-Event-ID
// header (WithHeader) of a new request to resume where an interrupted stream stopped.
func (s *Stream[T]) LastEventID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastID
}

// Close closes the connection. It is safe to call more than once and concurrently with an
// iteration, which then ends.
func (s *Stream[T]) Close() error {
	var err error
	s.closeOnce.Do(func() {
		s.call.idle.stop()
		s.call.cancel(nil)
		err = s.call.resp.Body.Close()
	})
	return err
}

// idleReader runs the idle timer only while a read is waiting for the network: the time the
// caller spends handling an event between two reads is not silence of the server.
type idleReader struct {
	r    io.Reader
	idle *idleTimer
}

func (r idleReader) Read(p []byte) (int, error) {
	r.idle.reset()
	n, err := r.r.Read(p)
	r.idle.stop()
	return n, err
}

// RawEvents yields the server-sent events as they arrive, without decoding their data. Use it
// instead of Events when you need the event names, ids or the heart-beats.
func (s *Stream[T]) RawEvents() iter.Seq2[SSEEvent, error] {
	return func(yield func(SSEEvent, error) bool) {
		s.mu.Lock()
		used := s.used
		s.used = true
		s.mu.Unlock()
		if used {
			yield(SSEEvent{}, &ValidationError{Param: "stream", Message: "the stream was already consumed: it can be iterated only once"})
			return
		}
		defer s.Close()
		for ev, err := range ParseSSE(idleReader{r: s.call.resp.Body, idle: s.call.idle}) {
			if err != nil {
				yield(SSEEvent{}, s.readError(err))
				return
			}
			s.mu.Lock()
			s.lastID = ev.ID
			s.mu.Unlock()
			if !yield(ev, nil) {
				return
			}
		}
	}
}

// readError classifies a failure while reading the stream body.
func (s *Stream[T]) readError(err error) error {
	c := s.call
	cause := context.Cause(c.ctx)
	if cause == nil || errors.Is(cause, errStreamIdle) || c.parent.Err() != nil {
		return transportError(c.parent, c.op, err, cause, c.idle.timeout)
	}
	// The stream was closed by Close while it was being read.
	return &ConnectionError{Op: c.op, Err: errors.New("the stream was closed")}
}

// Events yields the decoded events as they arrive. Heart-beats (events without data) are
// skipped. An event whose data does not decode is yielded as a *DecodeError and the iteration
// continues with the next event; a transport failure is yielded once and ends the iteration.
// The connection is closed when the loop ends or is left.
func (s *Stream[T]) Events() iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		var zero T
		for ev, err := range s.RawEvents() {
			if err != nil {
				yield(zero, err)
				return
			}
			if ev.Data == "" {
				continue // heart-beat ("ping")
			}
			var v T
			if err := json.Unmarshal(taggedEventData(ev, s.tagProperty), &v); err != nil {
				resp := s.call.resp
				decodeErr := &DecodeError{Op: s.call.op, StatusCode: resp.StatusCode, RequestID: resp.Header.Get("x-request-id"), Body: []byte(ev.Data), Err: err}
				if !yield(zero, decodeErr) {
					return
				}
				continue
			}
			if !yield(v, nil) {
				return
			}
		}
	}
}

// ChatStream is the event stream of a chat turn (Chat.CreateStream, Chat.AnswerStream,
// Chat.ListEvents) or of a batch (Batches.ListEvents).
type ChatStream struct {
	*Stream[ChatEvent]
}

func openChatStream(ctx context.Context, c *Client, spec *requestSpec, opts []Option) (*ChatStream, error) {
	s, err := openStream[ChatEvent](ctx, c, spec, opts)
	if err != nil {
		return nil, err
	}
	s.tagProperty = "type"
	return &ChatStream{Stream: s}, nil
}

// DialogID returns the id of the dialog from the X-Dialog-Id response header. A new dialog gets
// its id here: pass it as dialog_id of the next message to continue the conversation.
func (s *ChatStream) DialogID() string { return s.call.resp.Header.Get("X-Dialog-Id") }

// TurnNo returns the number of the turn from the X-Turn-No response header; ok is false when the
// header is missing or not a number.
func (s *ChatStream) TurnNo() (turn int64, ok bool) {
	n, err := strconv.ParseInt(strings.TrimSpace(s.call.resp.Header.Get("X-Turn-No")), 10, 64)
	return n, err == nil
}

// ErrStreamIncomplete is the cause of the *DecodeError that ChatStream.Accumulate returns when
// the stream ended before its done event: test for it with errors.Is.
var ErrStreamIncomplete = errors.New("the stream ended before completion (no done event)")

// ChatTurn is a chat turn assembled from its stream by ChatStream.Accumulate.
type ChatTurn struct {
	// DialogID is the dialog id from the X-Dialog-Id response header.
	DialogID string
	// TurnNo is the turn number from the X-Turn-No response header (0 when absent).
	TurnNo int64
	// Text is the answer: the text deltas, without the text of intermediate steps (those named
	// by a later progress event), replaced by the final answer of the done event when it has one.
	Text string
	// StopReason is done.stop_reason: end_turn, awaiting_user, budget, ...; "" when the stream
	// ended without a done event (Accumulate then returns an error).
	StopReason string
	// Done is the final event of the turn; nil when the stream ended without it.
	Done *ChatEventDone
	// Events are all events of the turn in order (heart-beats excluded).
	Events []ChatEvent
}

// Accumulate consumes the stream and assembles the turn. onEvent, when not nil, is called for
// every event as it arrives (print the delta of a *ChatEventText to show the answer live).
//
// When the turn fails, Accumulate returns the turn assembled so far together with the error: a
// *TurnError for an error event of the stream (the stream is still read to its end, as the API
// sends done after it), or the transport or decode error that interrupted the stream. A stream
// that ends without its done event is not a finished turn either: the error is then a
// *DecodeError that matches ErrStreamIncomplete, and the text is only what arrived.
func (s *ChatStream) Accumulate(onEvent func(ChatEvent)) (*ChatTurn, error) {
	turn := &ChatTurn{DialogID: s.DialogID()}
	turn.TurnNo, _ = s.TurnNo()

	type segment struct {
		step  *float64
		delta string
	}
	var segments []segment
	var firstErr error
	for ev, err := range s.Events() {
		if err != nil {
			var decodeErr *DecodeError
			if errors.As(err, &decodeErr) {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			firstErr = err
			break
		}
		turn.Events = append(turn.Events, ev)
		switch e := ev.Value.(type) {
		case *ChatEventText:
			segments = append(segments, segment{step: e.Step, delta: e.Delta})
		case *ChatEventProgress:
			kept := segments[:0]
			for _, seg := range segments {
				if seg.step == nil || *seg.step != e.Step {
					kept = append(kept, seg)
				}
			}
			segments = kept
		case *ChatEventError:
			if firstErr == nil {
				requestID := ""
				if e.RequestID != nil {
					requestID = *e.RequestID
				}
				firstErr = newTurnError(e.Error, requestID)
			}
		case *ChatEventDone:
			turn.Done = e
			if e.StopReason != nil {
				turn.StopReason = *e.StopReason
			}
		}
		if onEvent != nil {
			onEvent(ev)
		}
	}
	var text strings.Builder
	for _, seg := range segments {
		text.WriteString(seg.delta)
	}
	turn.Text = text.String()
	if turn.Done != nil && turn.Done.Answer != nil {
		turn.Text = *turn.Done.Answer
	}
	if turn.Done == nil && firstErr == nil {
		// The connection closed cleanly in the middle of the turn: the text so far is not the answer.
		resp := s.call.resp
		firstErr = &DecodeError{Op: s.call.op, StatusCode: resp.StatusCode, RequestID: resp.Header.Get("x-request-id"), Err: ErrStreamIncomplete}
	}
	return turn, firstErr
}
