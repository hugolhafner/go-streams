# KStream DSL

The KStream DSL provides a fluent, type-safe API for building stream processing topologies. All operations are package-level functions that take a `KStream` and return a new `KStream`.

## StreamsBuilder

`StreamsBuilder` is the entry point. It constructs the underlying topology as you chain operations.

```go
builder := kstream.NewStreamsBuilder()

// ... define sources, transformations, sinks ...

t := builder.Build() // returns *topology.Topology
```

Call `Build()` once and reuse the result for both `PrintTree()` and `NewApplication()`.

## Sources

Sources read records from a Kafka topic. Each source variant lets you control which serdes are used for deserialization.

### Stream

Reads both key and value as raw `[]byte`:

```go
s := kstream.Stream(builder, "my-topic")
// s is KStream[[]byte, []byte]
```

### StreamWithKeySerde

Deserializes the key with a custom serde, value stays `[]byte`:

```go
s := kstream.StreamWithKeySerde(builder, "my-topic", serde.String())
// s is KStream[string, []byte]
```

### StreamWithValueSerde

Deserializes the value with a custom serde, key stays `[]byte`:

```go
s := kstream.StreamWithValueSerde(builder, "my-topic", serde.JSON[Order]())
// s is KStream[[]byte, Order]
```

This is the most common variant - Kafka keys are often raw bytes while values are structured data.

### StreamWithSerde

Deserializes both key and value:

```go
s := kstream.StreamWithSerde(builder, "my-topic", serde.String(), serde.JSON[Order]())
// s is KStream[string, Order]
```

## Transformations

### Filter

Keeps records that match the predicate:

```go
func Filter[K, V any](s KStream[K, V], predicate func(context.Context, K, V) (bool, error)) KStream[K, V]
```

```go
valid := kstream.Filter(orders, func(ctx context.Context, k []byte, v Order) (bool, error) {
    return v.Amount > 0, nil
})
```

### FilterNot

Keeps records that do **not** match the predicate (inverse of `Filter`):

```go
func FilterNot[K, V any](s KStream[K, V], predicate func(context.Context, K, V) (bool, error)) KStream[K, V]
```

```go
invalid := kstream.FilterNot(orders, func(ctx context.Context, k []byte, v Order) (bool, error) {
    return v.Amount > 0, nil
})
```

### Map

Transforms both key and value, potentially changing their types:

```go
func Map[KIn, VIn, KOut, VOut any](
    s KStream[KIn, VIn],
    mapper func(context.Context, KIn, VIn) (KOut, VOut, error),
) KStream[KOut, VOut]
```

```go
summary := kstream.Map(orders, func(ctx context.Context, k []byte, v Order) (string, OrderSummary, error) {
    return v.UserID, OrderSummary{OrderID: v.ID, Amount: v.Amount}, nil
})
// summary is KStream[string, OrderSummary]
```

### MapValues

Transforms only the value, keeping the key unchanged:

```go
func MapValues[K, VIn, VOut any](
    s KStream[K, VIn],
    mapper func(context.Context, VIn) (VOut, error),
) KStream[K, VOut]
```

```go
amounts := kstream.MapValues(orders, func(ctx context.Context, v Order) (float64, error) {
    return v.Amount, nil
})
// amounts is KStream[[]byte, float64]
```

### MapKeys

Transforms only the key, keeping the value unchanged:

```go
func MapKeys[KIn, KOut, V any](
    s KStream[KIn, V],
    mapper func(context.Context, KIn) (KOut, error),
) KStream[KOut, V]
```

```go
rekeyed := kstream.MapKeys(orders, func(ctx context.Context, k []byte) (string, error) {
    return string(k), nil
})
// rekeyed is KStream[string, Order]
```

## Branching

`Branch` splits a stream into multiple sub-streams based on predicates. Each record goes to the **first** branch whose predicate returns `true`.

```go
func Branch[K, V any](s KStream[K, V], branches ...BranchConfig[K, V]) BranchOutput[K, V]
```

### Defining Branches

Use `NewBranch` for conditional branches and `DefaultBranch` for a catch-all:

```go
branches := kstream.Branch(
    orders,
    kstream.NewBranch("high-value", func(ctx context.Context, k []byte, v Order) (bool, error) {
        return v.Amount >= 1000, nil
    }),
    kstream.NewBranch("medium-value", func(ctx context.Context, k []byte, v Order) (bool, error) {
        return v.Amount >= 100, nil
    }),
    kstream.DefaultBranch[[]byte, Order]("other"),
)
```

### Accessing Branch Streams

Use `Get()` with the branch name:

```go
highValue := branches.Get("high-value")
medium := branches.Get("medium-value")
other := branches.Get("other")

kstream.ToWithValueSerde(highValue, "high-value-orders", serde.JSON[Order]())
kstream.ToWithValueSerde(medium, "medium-value-orders", serde.JSON[Order]())
kstream.ToWithValueSerde(other, "other-orders", serde.JSON[Order]())
```

## Terminal Operations

Terminal operations end a processing chain. They do not return a `KStream`.

### To

Writes records to a Kafka topic. Like sources, multiple variants control serialization.

```go
// Raw bytes (key and value must be []byte)
kstream.To(s, "output-topic")

// Custom key serde
kstream.ToWithKeySerde(s, "output-topic", serde.String())

// Custom value serde
kstream.ToWithValueSerde(s, "output-topic", serde.JSON[Order]())

// Both serdes
kstream.ToWithSerde(s, "output-topic", serde.String(), serde.JSON[Order]())
```

### ForEach

Applies a side-effect action to each record. Does not forward records downstream:

```go
func ForEach[K, V any](s KStream[K, V], action func(context.Context, K, V) error)
```

```go
kstream.ForEach(orders, func(ctx context.Context, k []byte, v Order) error {
    log.Printf("Processing order %s", v.ID)
    return nil
})
```

## Process (Custom Processors)

Plug in a custom `Processor` implementation using `Process`:

```go
func Process[KIn, VIn, KOut, VOut any](
    s KStream[KIn, VIn],
    supplier processor.Supplier[KIn, VIn, KOut, VOut],
) KStream[KOut, VOut]
```

```go
enriched := kstream.Process(orders, func() processor.Processor[[]byte, Order, []byte, EnrichedOrder] {
    return &myEnrichmentProcessor{}
})
```

See [Custom Processors](custom-processors.md) for details on implementing the `Processor` interface.
