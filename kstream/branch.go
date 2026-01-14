package kstream

import (
	"github.com/hugolhafner/go-streams/processor"
	"github.com/hugolhafner/go-streams/processor/builtins"
)

// BranchOutput represents the named outputs of a branch operation
type BranchOutput[K, V any] struct {
	streams map[string]KStream[K, V]
}

// Get returns the KStream for the given branch name
func (b BranchOutput[K, V]) Get(name string) KStream[K, V] {
	return b.streams[name]
}

// BranchConfig defines a single branch with a name and predicate
type BranchConfig[K, V any] struct {
	Name      string
	Predicate func(K, V) bool
}

// NewBranch creates a BranchConfig
func NewBranch[K, V any](name string, predicate func(K, V) bool) BranchConfig[K, V] {
	return BranchConfig[K, V]{
		Name:      name,
		Predicate: predicate,
	}
}

// DefaultBranch creates a catch-all branch config (always returns true)
func DefaultBranch[K, V any](name string) BranchConfig[K, V] {
	return BranchConfig[K, V]{
		Name:      name,
		Predicate: func(K, V) bool { return true },
	}
}

// Branch splits a stream into multiple streams based on predicates
// Each record goes to the first branch whose predicate matches
func Branch[K, V any](s KStream[K, V], branches ...BranchConfig[K, V]) BranchOutput[K, V] {
	branchNodeName := s.builder.nextName("BRANCH")

	predicates := make([]func(K, V) bool, len(branches))
	names := make([]string, len(branches))
	for i, b := range branches {
		predicates[i] = b.Predicate
		names[i] = b.Name
	}

	var supplier processor.Supplier[K, V, K, V] = func() processor.Processor[K, V, K, V] {
		return builtins.NewBranchProcessor(predicates, names)
	}

	s.builder.topology.AddProcessor(branchNodeName, supplier.ToUntyped(), s.nodeName)

	output := BranchOutput[K, V]{
		streams: make(map[string]KStream[K, V], len(branches)),
	}

	for _, b := range branches {
		passthroughName := s.builder.nextName("BRANCH-" + b.Name)
		var passthroughSupplier processor.Supplier[K, V, K, V] = func() processor.Processor[K, V, K, V] {
			return builtins.NewPassthroughProcessor[K, V]()
		}

		s.builder.topology.AddProcessorWithChildName(
			passthroughName,
			passthroughSupplier.ToUntyped(),
			branchNodeName,
			b.Name,
		)

		output.streams[b.Name] = KStream[K, V]{
			builder:  s.builder,
			nodeName: passthroughName,
		}
	}

	return output
}
