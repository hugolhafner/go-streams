package serde

type stringSerde struct{}

func String() Serde[string] {
	return stringSerde{}
}

func (s stringSerde) Serialize(topic string, value string) ([]byte, error) {
	return []byte(value), nil
}

func (s stringSerde) Deserialize(topic string, data []byte) (string, error) {
	return string(data), nil
}
