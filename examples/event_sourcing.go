package examples

import (
	"encoding/json"

	"github.com/hugolhafner/go-streams/kstream"
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
	valid := kstream.Filter(events, func(key []byte, event Event) bool {
		return event.Type != ""
	})

	branches := kstream.Branch(valid,
		kstream.NewBranch("order-created", func(key []byte, event Event) bool {
			return event.Type == "OrderCreated"
		}),
		kstream.NewBranch("order-updated", func(key []byte, event Event) bool {
			return event.Type == "OrderUpdated"
		}),
	)

	orderCreatedBranch := branches.Get("order-created")
	orderCreated := kstream.MapValues(orderCreatedBranch, func(event Event) OrderCreatedData {
		var data OrderCreatedData
		//nolint:errcheck
		json.Unmarshal(event.Data, &data)
		return data
	})

	kstream.ForEach(orderCreated, func(key []byte, data OrderCreatedData) {
		sendOrderCreatedEmail(data)
	})

	orderUpdatedBranch := branches.Get("order-updated")
	orderUpdated := kstream.MapValues(orderUpdatedBranch, func(event Event) OrderUpdatedData {
		var data OrderUpdatedData
		//nolint:errcheck
		json.Unmarshal(event.Data, &data)
		return data
	})

	kstream.ForEach(orderUpdated, func(key []byte, data OrderUpdatedData) {
		processOrderUpdate(data)
	})

	kstream.To(kstream.Map(orderUpdated, func(key []byte, data OrderUpdatedData) ([]byte, []byte) {
		value, _ := json.Marshal(data)
		return []byte(key), value
	}), "processed-order-created")

	kstream.ToWithValueSerde(orderUpdated, "processed-order-updated", serde.JSON[OrderUpdatedData]())

	t := builder.Build()
	t.PrintTree()
}
