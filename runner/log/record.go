package log

type ConsumerRecord struct {
	Key []byte

	Topic     string
	Partition int32
}

func (r ConsumerRecord) TopicPartition() TopicPartition {
	return TopicPartition{
		Topic:     r.Topic,
		Partition: r.Partition,
	}
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

func (tp TopicPartition) String() string {
	return tp.Topic + "-" + string(tp.Partition)
}
