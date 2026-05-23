package proto

// Message is the fundamental unit of data passed through the system.
type Message struct {
	Topic     string
	Partition int32
	Key       []byte
	Value     []byte
	Offset    int64
}

// TopicPartition identifies a specific partition within a topic.
type TopicPartition struct {
	Topic     string
	Partition int32
}
