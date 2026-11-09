# Tungo AI Go SDK (`github.com/TungoAI/tungo-sdk-go`)

Официальный Go SDK для публичного API платформы **Tungo AI** (Go 1.24+, **0 внешних зависимостей**).

## Установка

```bash
go get github.com/TungoAI/tungo-sdk-go
```

## Быстрый старт (для новичков — в 1 строчку)

```go
package main

import (
	"context"
	"fmt"
	"github.com/TungoAI/tungo-sdk-go"
)

func main() {
	client := tungo.NewClient(
		tungo.WithAPIKey("tg_live_..."),
		tungo.WithCompanyID("co_..."),
	)

	// 1. Задать вопрос ИИ-бухгалтеру
	res, _ := client.Ask(context.Background(), tungo.AskInput{Prompt: "Покажи дебиторку за октябрь"})
	fmt.Println(res.Reply)

	// 2. Автоматически обойти все страницы пагинации
	proposals, _ := client.Proposals.ListAll(context.Background(), nil)
	fmt.Println("Всего предложений:", len(proposals))

	// 3. Работа с деньгами в тиынах
	fmt.Println(tungo.FormatTiyn(tungo.ToTiyn(45000), "KZT")) // "45 000,00 ₸"
}
```

## Полный справочник ресурсов и методов

<!-- sdkgen:methods -->
Автоматически сгенерировано из OpenAPI-контракта Tungo Platform API: **13 ресурсов, 82 метода, 22 вебхука**.

| Ресурс | Описание | Методы |
| --- | --- | --- |
| `client.Workspaces` | Организации (workspace), доступные ключу API | `List` |
| `client.Companies` | Компании организации и их карточки | `List` · `Get` |
| `client.Platform` | Баланс платформы, книга движений, отчёты о расходе, лимиты, аудит и тарифы моделей | `ListUsageRates` · `GetAccount` · `GetAudit / GetAuditAll` · `GetLedger / GetLedgerAll` · `ListLimits` · `ListRequests / ListRequestsAll` · `GetUsage` |
| `client.ApiKeys` | Сведения о текущем ключе API и статистика его расхода | `GetCurrent` · `GetCurrentUsage` · `GetUsage` |
| `client.Webhooks` | Исходящие вебхуки организации (Svix): адреса, каталог событий, доставки и проверка подписи | `ListEndpoints / ListEndpointsAll` · `CreateEndpoint` · `GetEndpoint` · `UpdateEndpoint` · `DeleteEndpoint` · `ListEndpointAttempts / ListEndpointAttemptsAll` · `RotateEndpointSecret` · `TestEndpoint` · `ListEventTypes` · `ListMessages / ListMessagesAll` · `ListMessageAttempts / ListMessageAttemptsAll` · `ResendMessage` · `GetUsage` |
| `client.Chat` | Диалоги с ИИ-бухгалтером: вопросы, потоковый чат (SSE), подсчёт токенов и история | `List / ListAll` · `Create / CreateStream` · `CountTokens` · `Get` · `Update` · `Delete` · `Answer / AnswerStream` · `ListEvents` · `Stop` |
| `client.Proposals` | Предложения документов ИИ-бухгалтера: просмотр, одобрение записи в 1С и отклонение | `List / ListAll` · `ApproveBatch` · `Get` · `Approve` · `Reject` |
| `client.Feed` | Лента событий и предложений компании (страницы и живой поток SSE) | `Get / GetAll` · `ListEvents` |
| `client.Batches` | Пачки обработки банковских выписок, входящих ЭСФ и DirectBank | `List / ListAll` · `CreateBankStatement` · `CreateEsfPurchases` · `Get` · `ListEvents` · `Stop` · `GetSummary` · `ListStatements / ListStatementsAll` · `PollDirectbank` |
| `client.Esf` | Входящие и исходящие электронные счета-фактуры (ИС ЭСФ) | `GetIncoming / GetIncomingAll` · `GetOutgoing / GetOutgoingAll` |
| `client.Forms` | Налоговые формы (ФНО 100.00 и др.): сборка, проверка ИСНА и разрывы данных | `List` · `Build` · `ListDataGaps` · `GetIsna` · `ListNotes` |
| `client.Onec` | Состояние подключения компании к базе 1С | `GetStatus` |
| `client.Automations` | Автоматизации компании: входящие хуки, сценарии, цепочки шагов, запуски и уровень автономности ИИ | `PostHook` · `ListHooks` · `CreateHook` · `GetHook` · `UpdateHook` · `DeleteHook` · `RotateHook` · `TestHook` · `ListRuns / ListRunsAll` · `GetRun` · `CancelRun` · `RetryRun` · `ListScenarios` · `CreateScenario` · `GetScenario` · `PutScenario` · `UpdateScenario` · `DeleteScenario` · `RunScenario` · `GetState` · `PutState` · `GetAutonomy` · `PutAutonomy` |
<!-- /sdkgen:methods -->
