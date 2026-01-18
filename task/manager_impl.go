package task

import (
	"fmt"
	"sync"

	"github.com/hugolhafner/go-streams/kafka"
	"github.com/hugolhafner/go-streams/logger"
)

var _ Manager = (*managerImpl)(nil)

type managerImpl struct {
	tasks    map[kafka.TopicPartition]Task
	factory  Factory
	producer kafka.Producer

	mu     sync.RWMutex
	logger logger.Logger
}

func (m *managerImpl) OnAssigned(partitions []kafka.TopicPartition) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.logger.Debug("Assigning partitions", "partitions", partitions)

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

func (m *managerImpl) OnRevoked(partitions []kafka.TopicPartition) error {
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

func (m *managerImpl) Tasks() map[kafka.TopicPartition]Task {
	m.mu.RLock()
	defer m.mu.RUnlock()

	copied := make(map[kafka.TopicPartition]Task, len(m.tasks))
	for k, v := range m.tasks {
		copied[k] = v
	}

	return copied
}

func (m *managerImpl) TaskFor(partition kafka.TopicPartition) (Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	task, exists := m.tasks[partition]
	return task, exists
}

func (m *managerImpl) GetCommitOffsets() map[kafka.TopicPartition]kafka.Offset {
	m.mu.RLock()
	defer m.mu.RUnlock()

	offsets := make(map[kafka.TopicPartition]kafka.Offset)
	for p, task := range m.tasks {
		o, ok := task.CurrentOffset()
		if !ok {
			continue
		}

		offsets[p] = o
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

func NewManager(factory Factory, producer kafka.Producer, logger logger.Logger) Manager {
	return &managerImpl{
		tasks:    make(map[kafka.TopicPartition]Task),
		factory:  factory,
		producer: producer,
		logger:   logger.With("component", "task-manager"),
	}
}
