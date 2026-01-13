package task

import (
	"fmt"
	"sync"

	"github.com/hugolhafner/go-streams/runner/log"
)

var _ Manager = (*managerImpl)(nil)

type managerImpl struct {
	tasks    map[log.TopicPartition]Task
	factory  Factory
	producer log.Producer

	mu sync.RWMutex
}

func (m *managerImpl) OnAssigned(partitions []log.TopicPartition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, p := range partitions {
		if _, exists := m.tasks[p]; exists {
			continue
		}

		task, err := m.factory.CreateTask(p, m.producer)
		if err != nil {
			// cleanup previously created tasks
			for tp, t := range m.tasks {
				_ = t.Close()
				delete(m.tasks, tp)
			}

			return fmt.Errorf("create task: %w", err)
		}

		m.tasks[p] = task
	}

	return nil
}

func (m *managerImpl) OnRevoked(partitions []log.TopicPartition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for _, p := range partitions {
		task, exists := m.tasks[p]
		if !exists {
			continue
		}

		if err := task.Close(); err != nil {
			lastErr = fmt.Errorf("close task for partition %v: %w", p, err)
		}
		delete(m.tasks, p)
	}

	return lastErr
}

func (m *managerImpl) Tasks() map[log.TopicPartition]Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	copied := make(map[log.TopicPartition]Task, len(m.tasks))
	for k, v := range m.tasks {
		copied[k] = v
	}

	return copied
}

func (m *managerImpl) TaskFor(partition log.TopicPartition) (Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.tasks[partition]
	return task, exists
}

func (m *managerImpl) GetCommitOffsets() map[log.TopicPartition]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	offsets := make(map[log.TopicPartition]int64)
	for p, task := range m.tasks {
		offsets[p] = task.CurrentOffset()
	}

	return offsets
}

func (m *managerImpl) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for p, task := range m.tasks {
		if err := task.Close(); err != nil {
			lastErr = fmt.Errorf("close task for partition %v: %w", p, err)
		}
		delete(m.tasks, p)
	}

	return lastErr
}

func NewManager(factory Factory, producer log.Producer) Manager {
	return &managerImpl{
		tasks:    make(map[log.TopicPartition]Task),
		factory:  factory,
		producer: producer,
	}
}
