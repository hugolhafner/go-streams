package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kstream"
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

func orderProcessor() {
	builder := kstream.NewStreamsBuilder()
	raw := kstream.Stream[[]byte, []byte](builder, "orders")

	parsed := kstream.MapValues(raw, func(v []byte) Order {
		var o Order
		json.Unmarshal(v, &o)
		return o
	})

	valid := kstream.Filter(parsed, func(k []byte, v Order) bool {
		return v.ID != "" && v.Amount > 0
	})

	summary := kstream.Map(valid, func(k []byte, v Order) (string, OrderSummary) {
		return v.UserID, OrderSummary{
			OrderID: v.ID,
			Amount:  v.Amount,
		}
	})

	serialized := kstream.MapValues(summary, func(v OrderSummary) []byte {
		b, _ := json.Marshal(v)
		return b
	})

	output := kstream.MapKeys(serialized, func(k string) []byte {
		return []byte(k)
	})

	kstream.To(output, "order-summaries")
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
