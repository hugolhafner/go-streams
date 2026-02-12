package examples

import (
	"context"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
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
	valid := kstream.Filter(
		parsed, func(ctx context.Context, k []byte, v Order) (bool, error) {
			return v.ID != "" && v.Amount > 0, nil
		},
	)

	summary := kstream.Map(
		valid, func(ctx context.Context, k []byte, v Order) (string, OrderSummary, error) {
			return v.UserID, OrderSummary{
				OrderID: v.ID,
				Amount:  v.Amount,
			}, nil
		},
	)

	kstream.ToWithSerde(summary, "order-summaries", serde.String(), serde.JSON[OrderSummary]())
	t := builder.Build()
	t.PrintTree()

	client, err := kafka.NewKgoClient()
	if err != nil {
		panic(err)
	}
	defer client.Close()

	app, err := streams.NewApplication(
		client,
		t,
	)
	if err != nil {
		panic(err)
	}

	if err := app.RunWith(context.Background(), runner.NewSingleThreadedRunner()); err != nil {
		panic(err)
	}
}
