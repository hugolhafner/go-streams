package serde

var (
	_ Serde[[]byte]        = bytesSerde{}
	_ Serializer[[]byte]   = bytesSerde{}
	_ Deserializer[[]byte] = bytesSerde{}
)

type bytesSerde struct{}

func Bytes() Serde[[]byte] {
	return bytesSerde{}
}

func (s bytesSerde) Serialize(topic string, value []byte) ([]byte, error) {
	return value, nil
}

func (s bytesSerde) Deserialize(topic string, data []byte) ([]byte, error) {
	return data, nil
}
