package otel

import (
	"go.opentelemetry.io/otel/attribute"
)

const (
	AttrStreamID = attribute.Key("stream.id")

	AttrNodeName      = attribute.Key("stream.node.name")
	AttrNodeType      = attribute.Key("stream.node.type")
	AttrProcessStatus = attribute.Key("stream.process.status")
	AttrPollStatus    = attribute.Key("stream.poll.status")
	AttrProduceStatus = attribute.Key("stream.produce.status")
	AttrErrorAction   = attribute.Key("stream.error.action")
	AttrErrorNode     = attribute.Key("stream.error.node")
	AttrErrorPhase    = attribute.Key("stream.error.phase")
	AttrRunnerType    = attribute.Key("stream.runner.type")
	AttrEdgeSource    = attribute.Key("stream.edge.source")
	AttrEdgeTarget    = attribute.Key("stream.edge.target")
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
	NodeTypeSource    = "source"
	NodeTypeProcessor = "processor"
	NodeTypeSink      = "sink"
)

// Runner type values
const (
	RunnerTypeSingleThreaded = "single_threaded"
	RunnerTypePartitioned    = "partitioned"
)

// Rebalance attributes
const (
	AttrRebalanceType = attribute.Key("stream.rebalance.type")
)

// Rebalance type values
const (
	RebalanceTypeAssigned = "assigned"
	RebalanceTypeRevoked  = "revoked"
)

// Backpressure attributes
const (
	AttrBackpressureEvent = attribute.Key("stream.backpressure.event")
)

// Backpressure event values
const (
	BackpressureEventPaused  = "paused"
	BackpressureEventResumed = "resumed"
)
