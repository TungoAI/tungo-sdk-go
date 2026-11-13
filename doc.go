// Package tungo is the official Go client of the Tungo Platform API.
//
// Create one Client and share it; it is immutable and safe for concurrent use:
//
//	client := tungo.NewClient(
//		tungo.WithAPIKey(os.Getenv("TUNGO_API_KEY")),
//		tungo.WithCompanyID("co_..."),
//	)
//	turn, err := client.Ask(ctx, "Сколько мы должны поставщикам?")
//
// The API methods live on the resource services of the client (client.Chat,
// client.Proposals, client.Automations, ...). Every method takes a
// context.Context first and per-request options last. The same Option values
// configure the client (NewClient) and a single call.
//
// # Models
//
// Request and response models are generated from the API contract.
//
//   - A required property is a plain field.
//   - An optional property is a pointer (use Ptr), or a slice or map that is
//     omitted while it is nil. An empty, non-nil slice is sent as [].
//   - A property of a request model that the contract allows to be null is a
//     Nullable[T]: its zero value is omitted, Null[T]() sends null and
//     Value(v) sends v. In models that only ever come from the API such a
//     property is a pointer, nil meaning null or absent.
//   - A discriminated union (ChatEvent, WebhookEvent, ...) is a struct whose
//     Value field holds a pointer to one of the variant structs; use a type
//     switch. An unknown tag decodes into *UnknownVariant.
//   - A union of plain JSON shapes (bool or "unknown", string or number, ...)
//     is a struct with one pointer field per shape.
//
// Every model keeps the JSON it was decoded from: RawJSON returns it and
// ExtraFields returns the properties this SDK version has no field for.
// Enumerations are open string types, so a value added by the API later never
// fails to decode.
//
// # Errors
//
// Every error returned by this package implements Error. Use errors.As with
// *APIError (an HTTP error response; its Kind is the error.type of the API),
// *ConnectionError, *TimeoutError, *DecodeError, *ConfigError,
// *ValidationError, *PaginationError, *TurnError, *StreamError (an error
// event of the feed stream: reconnect) or *WebhookVerificationError. Cancelling the context of a call surfaces as an
// error that wraps context.Canceled.
//
// # Retries and timeouts
//
// A request is retried (DefaultMaxRetries times) on connection errors,
// timeouts and the statuses 408, 429, 500, 502, 503 and 504, but only when it
// is safe to repeat: GET, HEAD, PUT, DELETE, or a POST/PATCH that carries an
// Idempotency-Key. The SDK generates that key for the operations whose
// contract declares it. Timeouts are implemented with contexts, never with
// http.Client.Timeout, so they do not cut streams: an ordinary call has a
// total timeout per attempt, a stream only an idle timeout.
package tungo
