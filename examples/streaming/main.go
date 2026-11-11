// Command streaming prints the answer of the accountant while it is being written.
//
//	TUNGO_API_KEY=tg_... TUNGO_COMPANY_ID=co_... go run ./examples/streaming "Что с НДС за квартал?"
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"

	tungo "github.com/TungoAI/tungo-sdk-go"
)

func main() {
	question := strings.Join(os.Args[1:], " ")
	if question == "" {
		question = "Что нового по компании?"
	}
	// Ctrl-C cancels the context, which closes the stream.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client := tungo.NewClient()
	stream, err := client.Chat.CreateStream(ctx, tungo.ChatMessageRequest{Message: tungo.Ptr(question)})
	if err != nil {
		log.Fatal(err)
	}
	defer stream.Close()

	for event, err := range stream.Events() {
		if err != nil {
			log.Fatal(err)
		}
		switch e := event.Value.(type) {
		case *tungo.ChatEventText:
			fmt.Print(e.Delta)
		case *tungo.ChatEventError:
			log.Fatalf("the turn failed: %s", e.Error.Message)
		case *tungo.ChatEventDone:
			fmt.Println()
		}
	}
	fmt.Fprintln(os.Stderr, "dialog:", stream.DialogID())
}
