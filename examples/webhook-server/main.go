// Command webhook-server receives Tungo webhooks and verifies their signature.
//
//	TUNGO_WEBHOOK_SECRET=whsec_... go run ./examples/webhook-server
package main

import (
	"errors"
	"io"
	"log"
	"net/http"

	tungo "github.com/TungoAI/tungo-sdk-go"
)

func main() {
	// The client reads TUNGO_WEBHOOK_SECRET; verifying needs no API key.
	client := tungo.NewClient()

	http.HandleFunc("/webhooks/tungo", func(w http.ResponseWriter, r *http.Request) {
		// Verify the bytes exactly as they arrived: do not decode and re-encode the body first.
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		if err != nil {
			http.Error(w, "cannot read the body", http.StatusBadRequest)
			return
		}
		event, err := client.VerifyWebhook(body, r.Header)
		var verr *tungo.WebhookVerificationError
		if errors.As(err, &verr) {
			log.Printf("rejected a webhook: %s", verr.Reason)
			http.Error(w, "invalid signature", http.StatusBadRequest)
			return
		}
		switch e := event.Value.(type) {
		case *tungo.WebhookEventProposalWritten:
			log.Printf("proposal %s was written to 1C", e.Data.ProposalID)
		case *tungo.UnknownVariant:
			log.Printf("an event this SDK version does not know: %s", e.Tag)
		default:
			log.Printf("event %s", event.Tag())
		}
		// Deliveries repeat until they get a 2xx: deduplicate by the event id.
		w.WriteHeader(http.StatusNoContent)
	})
	log.Fatal(http.ListenAndServe("127.0.0.1:8080", nil))
}
