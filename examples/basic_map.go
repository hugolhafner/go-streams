package examples

import (
	"context"
	"log"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/serde"
)

type Order struct {
	ID     string  `json:"id"`
	Amount float64 `json:"amount"`
	UserID string  `json:"user_id"`
}

type OrderSummary struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

func BasicMap() {
	builder := kstream.NewStreamsBuilder()
	parsed := kstream.StreamWithValueSerde(builder, "orders", serde.JSON[Order]())
	valid := kstream.Filter(parsed, func(k []byte, v Order) bool {
		return v.ID != "" && v.Amount > 0
	})

	summary := kstream.Map(valid, func(k []byte, v Order) (string, OrderSummary) {
		return v.UserID, OrderSummary{
			OrderID: v.ID,
			Amount:  v.Amount,
		}
	})

	kstream.ToWithSerde(summary, "order-summaries", serde.String(), serde.JSON[OrderSummary]())
	t := builder.Build()
	t.PrintTree()

	app := streams.NewApplication(builder.Build(),
		streams.WithApplicationID("example-order-processor"),
		streams.WithBootstrapServers([]string{"localhost:9092"}),
	)

	if err := app.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
