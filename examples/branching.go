package examples

import (
	"context"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/internal/kafka"
	"github.com/hugolhafner/go-streams/internal/runner"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/serde"
)

const (
	highValueTopic = "high-value-orders"
	regularTopic   = "regular-orders"
)

func BranchProcessor() {
	builder := kstream.NewStreamsBuilder()
	raw := kstream.StreamWithValueSerde(builder, "orders", serde.JSON[Order]())

	valid := kstream.Filter(raw, func(k []byte, v Order) bool {
		return v.ID != "" && v.Amount > 0
	})

	branches := kstream.Branch(valid,
		kstream.NewBranch(highValueTopic, func(k []byte, v Order) bool {
			return v.Amount >= 1000
		}),
		kstream.DefaultBranch[[]byte, Order](regularTopic),
	)

	highValueOrders := branches.Get(highValueTopic)
	kstream.ToWithValueSerde(highValueOrders, highValueTopic, serde.JSON[Order]())

	regularOrders := branches.Get(regularTopic)
	kstream.ToWithValueSerde(regularOrders, regularTopic, serde.JSON[Order]())

	t := builder.Build()
	t.PrintTree()

	client, err := kafka.NewKgoClient()
	if err != nil {
		panic(err)
	}

	app, err := streams.NewApplication(
		client,
		builder.Build(),
		streams.WithApplicationID("example-order-processor"),
		streams.WithBootstrapServers([]string{"localhost:9092"}),
	)
	if err != nil {
		panic(err)
	}

	if err := app.RunWith(context.Background(), runner.NewSingleThreadedRunner()); err != nil {
		panic(err)
	}
}
