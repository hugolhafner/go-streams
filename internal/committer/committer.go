package committer

type Committer interface {
	TryCommit() bool
	UnlockCommit(ok bool)

	RecordProcessed(count int)
}
