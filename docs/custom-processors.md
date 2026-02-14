# Custom Processors

While the KStream DSL covers common operations (`Filter`, `Map`, `Branch`, etc.), you can implement the `Processor` interface for custom processing logic.

## Processor Interface

```go
type Processor[KIn, VIn, KOut, VOut any] interface {
    Init(ctx Context[KOut, VOut])
    Process(ctx context.Context, record *record.Record[KIn, VIn]) error
    Close() error
}
```

- **Init** - called once when the processor is created. Receives a `Context` for forwarding records downstream.
- **Process** - called for each incoming record. Use the context from `Init` to forward results.
- **Close** - called when the task is shut down. Clean up resources here.

## Context: Forwarding Records

The `Context` provided in `Init` has two methods:

```go
type Context[K, V any] interface {
    // Forward sends a record to all downstream children
    Forward(ctx context.Context, record *record.Record[K, V]) error

    // ForwardTo sends a record to a specific named child
    ForwardTo(ctx context.Context, childName string, record *record.Record[K, V]) error
}
```

Most processors use `Forward` to pass records to the next node. `ForwardTo` is used by the branch processor to route records to named children.

## Record Type

```go
type Record[K, V any] struct {
    Key   K
    Value V
    Metadata  // embedded: Topic, Partition, Offset, Timestamp, Headers
}
```

When forwarding, preserve the `Metadata` from the input record unless you have a reason to change it.

## Supplier Pattern

Processors are created via a `Supplier` - a factory function:

```go
type Supplier[KIn, VIn, KOut, VOut any] func() Processor[KIn, VIn, KOut, VOut]
```

**Why a factory?** Each partition task gets its own processor instance. The supplier is called once per task creation (e.g., on rebalance), so each partition gets independent state.

## Plugging In via KStream DSL

Use `kstream.Process` to add a custom processor to a stream:

```go
enriched := kstream.Process(orders, func() processor.Processor[[]byte, Order, []byte, EnrichedOrder] {
    return &EnrichmentProcessor{
        db: connectToDatabase(),
    }
})

// enriched is KStream[[]byte, EnrichedOrder]
// continue chaining: filter, map, sink, etc.
```

## Example: Enrichment Processor

```go
type EnrichedOrder struct {
    Order
    CustomerName string `json:"customer_name"`
}

type EnrichmentProcessor struct {
    ctx processor.Context[[]byte, EnrichedOrder]
    db  *sql.DB
}

func (p *EnrichmentProcessor) Init(ctx processor.Context[[]byte, EnrichedOrder]) {
    p.ctx = ctx
}

func (p *EnrichmentProcessor) Process(ctx context.Context, r *record.Record[[]byte, Order]) error {
    name, err := p.db.QueryRowContext(ctx,
        "SELECT name FROM customers WHERE id = ?", r.Value.UserID,
    ).Scan()
    if err != nil {
        return err // error handler decides what to do
    }

    return p.ctx.Forward(ctx, &record.Record[[]byte, EnrichedOrder]{
        Key: r.Key,
        Value: EnrichedOrder{
            Order:        r.Value,
            CustomerName: name.(string),
        },
        Metadata: r.Metadata,
    })
}

func (p *EnrichmentProcessor) Close() error {
    return p.db.Close()
}
```

Wire it into a topology:

```go
builder := kstream.NewStreamsBuilder()
orders := kstream.StreamWithValueSerde(builder, "orders", serde.JSON[Order]())

enriched := kstream.Process(orders, func() processor.Processor[[]byte, Order, []byte, EnrichedOrder] {
    db, _ := sql.Open("postgres", "...")
    return &EnrichmentProcessor{db: db}
})

kstream.ToWithValueSerde(enriched, "enriched-orders", serde.JSON[EnrichedOrder]())
```

## Key Points

- Each task (partition) gets its own processor instance via the supplier
- Store the `Context` from `Init` to use in `Process`
- Always forward with `record.Record` - preserve `Metadata` for offset tracking
- Return errors from `Process` - the configured [error handler](error-handling.md) will decide the action
- Clean up resources (DB connections, files, etc.) in `Close`
