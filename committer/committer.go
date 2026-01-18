package committer

type Committer interface {
	C() chan struct{}
	RecordProcessed(count int)
	Close()
}
