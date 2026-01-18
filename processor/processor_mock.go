package processor

import (
	"github.com/hugolhafner/go-streams/record"
	"github.com/stretchr/testify/mock"
)

var _ Context[any, any] = (*MockContext[any, any])(nil)

type MockContext[K, V any] struct {
	mock.Mock
}

func NewMockContext[K, V any]() *MockContext[K, V] {
	return &MockContext[K, V]{}
}

func (c *MockContext[K, V]) Forward(record *record.Record[K, V]) error {
	args := c.Mock.Called(record)
	return args.Error(0)
}

func (c *MockContext[K, V]) ForwardTo(childName string, record *record.Record[K, V]) error {
	args := c.Mock.Called(childName, record)
	return args.Error(0)
}
