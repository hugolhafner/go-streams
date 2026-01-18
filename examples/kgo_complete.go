package examples

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/hugolhafner/go-streams"
	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/kstream"
	"github.com/hugolhafner/go-streams/plugins/zaplogger"
	"github.com/hugolhafner/go-streams/runner"
	"github.com/hugolhafner/go-streams/serde"
	"go.uber.org/zap"
)

// {"id": "order1", "amount": 100.0, "user_id": "user1"}
func filterInvalidOrders(k []byte, v Order) bool {
	keep := v.ID != "" && v.Amount > 0
	if !keep {
		zap.L().Warn("Invalid order", zap.String("key", string(k)), zap.Any("value", v))
	} else {
		zap.L().Debug("Valid order", zap.String("key", string(k)), zap.Any("value", v))
	}

	return keep
}

func processOrder(k []byte, v Order) (string, OrderSummary) {
	zap.L().Debug("Processing order", zap.String("key", string(k)), zap.Any("value", v))
	return v.UserID, OrderSummary{
		OrderID: v.ID,
		Amount:  v.Amount,
	}
}

func KgoComplete() {
	l, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	zap.ReplaceGlobals(l)
	klogger := zaplogger.New(l)

	builder := kstream.NewStreamsBuilder()
	parsed := kstream.StreamWithValueSerde(builder, "example-kgo-complete-input", serde.JSON[Order]())
	valid := kstream.Filter(parsed, filterInvalidOrders)
	summary := kstream.Map(valid, processOrder)
	kstream.ToWithSerde(summary, "example-kgo-complete-output", serde.String(), serde.JSON[OrderSummary]())

	t := builder.Build()
	t.PrintTree()

	client, err := kafka.NewKgoClient(
		// kafka.WithLogger(klogger),
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
		streams.WithLogger(klogger),
	)
	if err != nil {
		panic(err)
	}

	defer app.Close()

	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		<-ch
		l.Info("Received termination signal, shutting down...")
		app.Close()
	}()

	if err := app.RunWith(context.Background(), runner.NewSingleThreadedRunner()); err != nil {
		panic(err)
	}
}
