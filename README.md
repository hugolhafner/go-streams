# go-streams

Just because you have to respect Java for its enterprise usage, doesn't mean you have to use it.

> [!CAUTION]  
> Note, this library provides no guarantees of a stable API following semantic conventions prior to version v1.x

## Non Goals

- Does not aim to support Exactly Once semantics

## Testing Strategy

```shell
mise run test[-json]
mise run test-integration[-json]
mise run test-e2e[-json]
```

### Overview

| Feature           | Unit              | Integration              | E2E                   |
|-------------------|-------------------|--------------------------|-----------------------|
| Data Dependencies | No                | Yes                      | Yes                   |
| External Services | No                | No                       | Yes                   |
| Mocks             | Most dependencies | Usually None             | None                  |
| Tested Interface  | Go Package        | Go Package               | Input / Output Topics |
| File Naming       | *_test.go         | *_integration_test.go    | e2e/*_test.go         |
| Go Build Tag      | `//go:build unit` | `//go:build integration` | `//go:build e2e`      |
