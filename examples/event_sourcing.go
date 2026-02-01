package examples

import (
	"context"
	"encoding/json"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
)

type Event struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type OrderCreatedData struct {
	OrderID string  `json:"order_id"`
	Amount  float64 `json:"amount"`
}

type OrderUpdatedData struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func sendOrderCreatedEmail(data OrderCreatedData) {
	// Implementation to send email
}

func processOrderUpdate(data OrderUpdatedData) {
	// Implementation to process order update
}

func EventSourcing() {
	builder := kstream.NewStreamsBuilder()
	events := kstream.StreamWithValueSerde(builder, "order-events", serde.JSON[Event]())
	valid := kstream.Filter(
		events, func(ctx context.Context, key []byte, event Event) (bool, error) {
			return event.Type != "", nil
		},
	)

	branches := kstream.Branch(
		valid,
		kstream.NewBranch(
			"order-created", func(ctx context.Context, key []byte, event Event) (bool, error) {
				return event.Type == "OrderCreated", nil
			},
		),
		kstream.NewBranch(
			"order-updated", func(ctx context.Context, key []byte, event Event) (bool, error) {
				return event.Type == "OrderUpdated", nil
			},
		),
	)

	orderCreatedBranch := branches.Get("order-created")
	orderCreated := kstream.MapValues(
		orderCreatedBranch, func(ctx context.Context, event Event) (OrderCreatedData, error) {
			var data OrderCreatedData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return OrderCreatedData{}, err
			}

			return data, nil
		},
	)

	kstream.ForEach(
		orderCreated, func(ctx context.Context, key []byte, data OrderCreatedData) error {
			sendOrderCreatedEmail(data)
			return nil
		},
	)

	orderUpdatedBranch := branches.Get("order-updated")
	orderUpdated := kstream.MapValues(
		orderUpdatedBranch, func(ctx context.Context, event Event) (OrderUpdatedData, error) {
			var data OrderUpdatedData
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return OrderUpdatedData{}, err
			}

			return data, nil
		},
	)

	kstream.ForEach(
		orderUpdated, func(ctx context.Context, key []byte, data OrderUpdatedData) error {
			processOrderUpdate(data)
			return nil
		},
	)

	kstream.To(
		kstream.Map(
			orderUpdated, func(ctx context.Context, key []byte, data OrderUpdatedData) ([]byte, []byte, error) {
				value, _ := json.Marshal(data)
				return key, value, nil
			},
		), "processed-order-created",
	)

	kstream.ToWithValueSerde(orderUpdated, "processed-order-updated", serde.JSON[OrderUpdatedData]())

	t := builder.Build()
	t.PrintTree()

	client, err := kafka.NewKgoClient()
	if err != nil {
		panic(err)
	}

	app, err := streams.NewApplication(
		client,
		builder.Build(),
	)
	if err != nil {
		panic(err)
	}

	if err := app.RunWith(context.Background(), runner.NewSingleThreadedRunner()); err != nil {
		panic(err)
	}
}
