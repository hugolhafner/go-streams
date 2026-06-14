  ---                                                                                                                                                                                                                                                                                                                            
  Struct & Interface Separation Analysis: go-streams
                                                                                                                                                                                                                                                                                                                                 
  Overview                                                                                                                                                                                                                                                                                                                       

  The codebase defines 23 interfaces across 8 packages and ~40 structs (non-test). The overall design quality is strong - small interfaces, proper adapter patterns, and a clean typed/untyped duality for generics. However, there are meaningful inconsistencies and a few structural issues.

  ---
  Final List of Findings

  1. Typo in Public API: ToUntypedDeserialser

  Location: serde/adapter.go:41
  Severity: High (breaking change to fix later)

  The function ToUntypedDeserialser is missing an i - it should be ToUntypedDeserialiser. This typo is used in 70+ call sites across tests and production code (kstream/stream.go:36-37). Fixing it later is a breaking API change. The companion function ToUntypedSerialiser is spelled correctly, making this inconsistency
  visible.

  ---
  2. Options Pattern Inconsistency Across Packages

  Severity: Medium

  Three different patterns are used:

  ┌─────────────────────┬───────────────────────────────────────────────┬───────────────────────────────────┐
  │       Package       │                    Pattern                    │               Type                │
  ├─────────────────────┼───────────────────────────────────────────────┼───────────────────────────────────┤
  │ stream.go           │ type ConfigOption func(*Config)               │ Function-based                    │
  ├─────────────────────┼───────────────────────────────────────────────┼───────────────────────────────────┤
  │ kafka/client_kgo.go │ type KgoOption func(*KgoClientConfig)         │ Function-based                    │
  ├─────────────────────┼───────────────────────────────────────────────┼───────────────────────────────────┤
  │ task/factory.go     │ type FactoryOption func(*topologyTaskFactory) │ Function-based                    │
  ├─────────────────────┼───────────────────────────────────────────────┼───────────────────────────────────┤
  │ runner/options.go   │ type SingleThreadedOption interface { ... }   │ Interface-based with struct impls │
  └─────────────────────┴───────────────────────────────────────────────┴───────────────────────────────────┘

  The runner package uses a fundamentally different approach (interface-based options with concrete struct types implementing applySingleThreaded/applyPartitioned) while every other package uses simple func(*Config) closures. The runner pattern has merit (compile-time separation of which options apply to which runner),
  but the inconsistency across the codebase hurts discoverability.

  ---
  3. Factory Pattern Inconsistency: Type Alias vs Interface

  Severity: Medium

  ┌─────────────────────────┬───────────────┬────────────────────────────────────────────────────────────────────────────────────────────┐
  │         Package         │ Factory Type  │                                         Definition                                         │
  ├─────────────────────────┼───────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
  │ runner/runner.go:17     │ Type alias    │ type Factory = func(Topology, task.Factory, Consumer, Producer, Telemetry) (Runner, error) │
  ├─────────────────────────┼───────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
  │ task/factory.go:13      │ Interface     │ type Factory interface { CreateTask(partition, producer) (Task, error) }                   │
  ├─────────────────────────┼───────────────┼────────────────────────────────────────────────────────────────────────────────────────────┤
  │ processor/supplier.go:3 │ Function type │ type Supplier[...] func() Processor[...]                                                   │
  └─────────────────────────┴───────────────┴────────────────────────────────────────────────────────────────────────────────────────────┘

  runner.Factory is a = type alias (no new type created), task.Factory is an interface, and processor.Supplier is a named function type. These three serve the same conceptual purpose (deferred creation) but use three different Go mechanisms. runner.Factory being a type alias is the weakest - it can't be extended, can't
  have methods, and doesn't create a distinct type for tooling.

  ---
  4. Exported Method Returns Unexported Type: Topology.SourceNodes()

  Location: topology/topology.go:144
  Severity: High

  func (t *Topology) SourceNodes() []*sourceNode

  This exported method returns []*sourceNode where sourceNode is unexported. This is a Go anti-pattern - external packages can call this method but can't name the return type. It should return []SourceNode (the interface). The companion SinkNodes() at line 174 returns []Node instead of []SinkNode, which is a different
  inconsistency - it uses the wrong level of the interface hierarchy.

  ---
  5. Constructor Return Type Inconsistency

  Severity: Medium

  ┌──────────────────────────────┬───────────────────────┬───────────────────┐
  │         Constructor          │        Returns        │       Style       │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewApplication(...)          │ (*Application, error) │ Concrete          │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewKgoClient(...)            │ (*KgoClient, error)   │ Concrete          │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewStreamsBuilder()          │ *StreamsBuilder       │ Concrete          │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewTelemetry(...)            │ (*Telemetry, error)   │ Concrete          │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewManager(...)              │ Manager               │ Interface         │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewTopologyTaskFactory(...)  │ (Factory, error)      │ Interface         │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewNoopLogger()              │ Logger                │ Interface         │
  ├──────────────────────────────┼───────────────────────┼───────────────────┤
  │ NewSingleThreadedRunner(...) │ Factory               │ Type alias (func) │
  └──────────────────────────────┴───────────────────────┴───────────────────┘

  The Go proverb is "accept interfaces, return structs." NewManager, NewTopologyTaskFactory, and NewNoopLogger return interfaces, hiding the concrete type. While sometimes desirable, this makes the codebase inconsistent - some constructors let you access concrete methods, others don't.

  ---
  6. Export Visibility Inconsistencies

  Severity: Medium

  Within task/ package:
  - TopologyTask - exported
  - topologyTaskFactory - unexported
  - managerImpl - unexported

  TopologyTask is the only concrete implementation of Task, yet it's exported while the other implementations in the same package are unexported. Since TopologyTask is always created via Factory and the constructor NewTopologyTaskFactory returns the Factory interface, there's no reason to export the struct.

  Within logger/ package:
  - LevelWrapper - exported (logger/wrapper.go:3)
  - NoopLogger - exported (intentional, users implement Base)

  LevelWrapper is an internal decorator that wraps Base into Logger. It's only created by WrapLogger(). Exporting it leaks an implementation detail - users should work with the Logger interface, not LevelWrapper directly.

  Within runner/ package:
  - SingleThreaded - exported
  - PartitionedRunner - exported
  - BaseConfig, SingleThreadedConfig, PartitionedConfig - exported

  The runner structs are exported but returned behind the Runner interface via Factory. The config structs are exported but only consumed by unexported apply* methods. None of these need to be exported.

  ---
  7. Topology Is Always a Concrete Struct, Never Abstracted

  Severity: Low-Medium

  *topology.Topology is passed as a concrete pointer through:
  - runner.Factory signature
  - task.NewTopologyTaskFactory()
  - stream.NewApplication()
  - kstream.StreamsBuilder.Build() returns it

  Every other major dependency in the system is abstracted behind an interface (kafka.Client, kafka.Consumer, kafka.Producer, task.Factory, task.Manager, runner.Runner, logger.Logger, errorhandler.Handler). Topology is the exception. This makes it impossible to mock the topology in tests without using the real
  implementation.

  ---
  8. Topology Leaks Internal State

  Location: topology/topology.go:113, 129-131
  Severity: Low-Medium

  func (t *Topology) Nodes() map[string]Node { return t.nodes }       // returns internal map
  func (t *Topology) Children(parent string) []string { return t.edges[parent] }  // returns internal slice

  Both methods return direct references to internal data structures. Callers can mutate the topology's state by modifying the returned map/slice. NamedEdges() at line 117 correctly returns a copy of the map, showing awareness of the issue - but Nodes() and Children() don't follow the same pattern.

  ---
  9. Topology.PrintTree() Has Side Effects

  Location: topology/topology.go:189
  Severity: Low

  PrintTree() calls fmt.Printf directly, printing to stdout. In a library, this is an anti-pattern - it should accept an io.Writer parameter or return a string. This makes it unusable in structured logging, testing, or non-stdout contexts.

  ---
  10. British Spelling in Public API

  Severity: Low (cosmetic, but permanent)

  The serde package uses British English:
  - Serialiser / Deserialiser (British -iser)
  - Serialise / Deserialise (British -ise)

  The Go ecosystem overwhelmingly uses American English (Serializer, Serialize). This will be unexpected for most Go developers and creates friction with IDE autocomplete expectations. Combined with the Deserialser typo (#1), the naming in this package is the weakest part of the public API.

  ---
  11. Missing Compile-Time Interface Checks in serde/

  Severity: Low

  The codebase consistently uses var _ Interface = (*Struct)(nil) checks - except in the serde package. None of the serde implementations have them:
  - bytesSerde - no check for Serde[[]byte]
  - stringSerde - no check for Serde[string]
  - jsonSerde[T] - no check for Serde[T]
  - protobufSerde[T] - no check for Serde[T]
  - Adapter types - no checks for UntypedDeserialiser / UntypedSerialiser / UntypedSerde

  Note: Generic type parameter constraints may make some of these impossible to express as var _ checks, but the non-generic adapters (deserializerAdapter, etc.) could have them.

  ---
  12. kafka.Consumer Interface Size

  Location: kafka/client.go:20-29
  Severity: Low

  Consumer has 8 methods spanning subscription, polling, committing, partition control, and metadata. While all are cohesive to "consuming from Kafka," the interface is large by Go standards. This means any mock must implement all 8 methods even if a test only exercises Poll. The mock at kafka/mock/ handles this, but
  consumer code that only needs Poll + Commit is forced to depend on the full interface.

  ---
  13. record Package Is Arguably Unnecessary

  Severity: Low (structural)

  The record/ package contains exactly 3 types (Metadata, Record[K,V], UntypedRecord) and 2 functions across 2 files. It exists solely to avoid circular dependencies between kafka and processor. This is a common Go workaround but creates an extra package that developers must discover. The types are simple value objects
  that could potentially be co-located with processor if the import cycle were resolved differently.

  ---
  Summary Matrix

  ┌─────┬─────────────────────────────────────────────────────────────────┬────────────┬────────────────┐
  │  #  │                             Finding                             │  Severity  │    Category    │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 1   │ ToUntypedDeserialser typo in public API                         │ High       │ Naming         │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 2   │ Options pattern inconsistency (func vs interface)               │ Medium     │ Consistency    │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 3   │ Factory pattern inconsistency (alias vs interface vs func type) │ Medium     │ Consistency    │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 4   │ SourceNodes() returns unexported *sourceNode                    │ High       │ Encapsulation  │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 5   │ Constructor return type inconsistency (concrete vs interface)   │ Medium     │ Consistency    │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 6   │ Export visibility inconsistencies across packages               │ Medium     │ Encapsulation  │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 7   │ Topology has no interface abstraction                           │ Low-Medium │ Testability    │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 8   │ Topology.Nodes()/Children() leak internal state                 │ Low-Medium │ Encapsulation  │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 9   │ Topology.PrintTree() writes to stdout                           │ Low        │ Side effects   │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 10  │ British spelling in serde public API                            │ Low        │ Naming         │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 11  │ Missing var _ compile-time checks in serde                      │ Low        │ Consistency    │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 12  │ kafka.Consumer interface has 8 methods                          │ Low        │ Interface size │
  ├─────┼─────────────────────────────────────────────────────────────────┼────────────┼────────────────┤
  │ 13  │ record package is very thin                                     │ Low        │ Structure      │
  └─────┴─────────────────────────────────────────────────────────────────┴────────────┴────────────────┘

  ---
  What the Codebase Does Well

  To be fair, the design is strong overall:

  - Typed/untyped duality with adapter pattern is elegant and well-executed
  - Interface sizes are generally small and behavior-focused (1-4 methods typical)
  - Consumer-side interface definition is applied consistently
  - var _ compile-time checks are used in most packages
  - Error handler design (Action types, HandlerFunc adapter, PhaseRouter, decorators) is excellent
  - Private implementations behind public interfaces in task/, topology/node.go, processor/adapter.go
  - No interface pollution - every interface serves a clear, necessary purpose
