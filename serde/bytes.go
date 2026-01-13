package serde

var (
	_ Serde[[]byte]        = bytesSerde{}
	_ Serialiser[[]byte]   = bytesSerde{}
	_ Deserialiser[[]byte] = bytesSerde{}
)

type bytesSerde struct{}

func Bytes() Serde[[]byte] {
	return bytesSerde{}
}

func (s bytesSerde) Serialise(topic string, value []byte) ([]byte, error) {
	return value, nil
}

func (s bytesSerde) Deserialise(topic string, data []byte) ([]byte, error) {
	return data, nil
}
