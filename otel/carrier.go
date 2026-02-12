package otel

import "github.com/hugolhafner/go-streams/kafka"

type KafkaHeadersCarrier struct {
	Headers *[]kafka.Header
}

func NewKafkaHeadersCarrier(headers *[]kafka.Header) KafkaHeadersCarrier {
	return KafkaHeadersCarrier{Headers: headers}
}

func (c KafkaHeadersCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c KafkaHeadersCarrier) Set(key, value string) {
	// Kafka can have multiple headers with the same key, overwrite all existing headers with the same key
	// or add new one
	found := false
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(value)
			found = true
		}
	}

	if !found {
		*c.Headers = append(*c.Headers, kafka.Header{Key: key, Value: []byte(value)})
	}
}

func (c KafkaHeadersCarrier) Keys() []string {
	keys := make([]string, len(*c.Headers))
	for i, h := range *c.Headers {
		keys[i] = h.Key
	}
	return keys
}
