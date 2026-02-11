# Serialization

go-streams uses a serde (serializer/deserializer) system to convert between Go types and Kafka's raw bytes. Serdes are defined in the `serde` package.

## Interfaces

```go
// Full serde — implements both directions
type Serde[T any] interface {
    Serialiser[T]
    Deserialiser[T]
}

// Serialise converts a Go value to bytes for writing to Kafka
type Serialiser[T any] interface {
    Serialise(topic string, value T) ([]byte, error)
}

// Deserialise converts bytes from Kafka into a Go value
type Deserialiser[T any] interface {
    Deserialise(topic string, data []byte) (T, error)
}
```

Both methods receive the `topic` name, which allows topic-aware serialization (e.g., schema registry integration).

## Where Serdes Are Used

| Location | Interface Used | Purpose |
|----------|---------------|---------|
| `StreamWith*Serde` sources | `Deserialiser[T]` | Deserialize incoming records |
| `ToWith*Serde` sinks | `Serde[T]` | Serialize outgoing records |

Source functions only need a `Deserialiser` since they only read. Sink functions require a full `Serde` (which includes `Serialiser`).

## Built-in Serdes

All built-in serdes implement `Serde[T]` (both serialization and deserialization).

### Bytes

Pass-through — no transformation. This is the default when no serde is specified.

```go
serde.Bytes() // Serde[[]byte]
```

### String

Converts between `string` and `[]byte`:

```go
serde.String() // Serde[string]
```

### JSON

Serializes/deserializes using `encoding/json`:

```go
serde.JSON[Order]() // Serde[Order]
```

Works with any type that `json.Marshal`/`json.Unmarshal` can handle. This is the most common serde for structured data.

### Protobuf

Serializes/deserializes using `google.golang.org/protobuf/proto`:

```go
serde.Protobuf[*pb.MyMessage]() // Serde[*pb.MyMessage]
```

The type parameter **must** be a pointer to a proto message (implements `proto.Message`). The deserializer uses reflection to create new instances.

## Implementing a Custom Serde

Implement the `Serde[T]` interface:

```go
type AvroSerde[T any] struct {
    schema string
}

func (s AvroSerde[T]) Serialise(topic string, value T) ([]byte, error) {
    // encode value using Avro schema
}

func (s AvroSerde[T]) Deserialise(topic string, data []byte) (T, error) {
    // decode data using Avro schema
}
```

If you only need one direction, implement just `Serialiser[T]` or `Deserialiser[T]`.

## Usage Examples

Source with JSON values:

```go
orders := kstream.StreamWithValueSerde(builder, "orders", serde.JSON[Order]())
```

Source with both key and value serdes:

```go
orders := kstream.StreamWithSerde(builder, "orders", serde.String(), serde.JSON[Order]())
```

Sink with serdes:

```go
kstream.ToWithSerde(stream, "output", serde.String(), serde.JSON[OrderSummary]())
```
