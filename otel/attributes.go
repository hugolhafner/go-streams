package otel

import (
	"go.opentelemetry.io/otel/attribute"
)

const (
	AttrNodeName      = attribute.Key("stream.node.name")
	AttrNodeType      = attribute.Key("stream.node.type")
	AttrProcessStatus = attribute.Key("stream.process.status")
	AttrPollStatus    = attribute.Key("stream.poll.status")
	AttrProduceStatus = attribute.Key("stream.produce.status")
	AttrErrorAction   = attribute.Key("stream.error.action")
	AttrErrorNode     = attribute.Key("stream.error.node")
	AttrErrorPhase    = attribute.Key("stream.error.phase")
	AttrRunnerType    = attribute.Key("stream.runner.type")
)

// Process status values
const (
	StatusSuccess = "success"
	StatusDropped = "dropped"
	StatusDLQ     = "dlq"
	StatusFailed  = "failed"
	StatusError   = "error"
)

// Node type values
const (
	NodeTypeProcessor = "processor"
	NodeTypeSink      = "sink"
)

// Runner type values
const (
	RunnerTypeSingleThreaded = "single_threaded"
	RunnerTypePartitioned    = "partitioned"
)
