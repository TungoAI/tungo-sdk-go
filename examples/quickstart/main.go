// Command quickstart asks the accountant one question and prints the answer.
//
//	TUNGO_API_KEY=tg_... TUNGO_COMPANY_ID=co_... go run ./examples/quickstart
package main

import (
	"context"
	"fmt"
	"log"

	tungo "github.com/TungoAI/tungo-sdk-go"
)

func main() {
	// NewClient reads TUNGO_API_KEY, TUNGO_COMPANY_ID and TUNGO_WORKSPACE_ID from the environment.
	client := tungo.NewClient()

	turn, err := client.Ask(context.Background(), "Сколько денег на расчётном счёте?")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(turn.Answer)
}
