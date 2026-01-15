package examples

import (
	"context"
	"os"
	"os/signal"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/internal/kafka"
	"github.com/hugolhafner/go-streams/internal/runner"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/plugins/zaplogger"
	"github.com/hugolhafner/go-streams/serde"
	"go.uber.org/zap"
)

// {"id": "order1", "amount": 100.0, "user_id": "user1"}

func KgoComplete() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(l)

	builder := kstream.NewStreamsBuilder()
	parsed := kstream.StreamWithValueSerde(builder, "example-kgo-complete-input", serde.JSON[Order]())
	valid := kstream.Filter(parsed, func(k []byte, v Order) bool {
		keep := v.ID != "" && v.Amount > 0
		if !keep {
			l.Warn("Invalid order", zap.String("key", string(k)), zap.Any("value", v))
		} else {
			l.Debug("Valid order", zap.String("key", string(k)), zap.Any("value", v))
		}

		return keep
	})

	summary := kstream.Map(valid, func(k []byte, v Order) (string, OrderSummary) {
		l.Debug("Processing order", zap.String("key", string(k)), zap.Any("value", v))
		return v.UserID, OrderSummary{
			OrderID: v.ID,
			Amount:  v.Amount,
		}
	})

	kstream.ToWithSerde(summary, "example-kgo-complete-output", serde.String(), serde.JSON[OrderSummary]())
	t := builder.Build()
	t.PrintTree()

	client, err := kafka.NewKgoClient(
		kafka.WithGroupID("example-kgo-complete"),
		kafka.WithBootstrapServers([]string{"localhost:19092"}),
	)
	if err != nil {
		panic(err)
	}

	defer client.Close()

	app, err := streams.NewApplication(
		client,
		builder.Build(),
		streams.WithApplicationID("example-order-processor"),
		streams.WithBootstrapServers([]string{"localhost:19092"}),
		streams.WithLogger(zaplogger.New(l)),
	)
	if err != nil {
		panic(err)
	}

	defer app.Close()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, os.Kill)
		<-ch
		l.Info("Received termination signal, shutting down...")
		app.Close()
	}()

	if err := app.RunWith(context.Background(), runner.NewSingleThreadedRunner()); err != nil {
		panic(err)
	}
}
