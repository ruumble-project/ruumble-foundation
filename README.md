# Ruumble Foundation

What the Ruumble building stands on: a clustered voice server in Go that replaces the Mumble core underneath.
It runs on three nodes, speaks Mumble's protocol (clients from Mumble 1.5 on), forwards voice between nodes, and
keeps all stored state replicated through Raft, so a node can fail or be updated without the building going quiet.

Nothing is here yet. The design lives in the site office,
[`ruumble-project/ruumble-site-office`](https://github.com/ruumble-project/ruumble-site-office):

- [ADR-0029](https://github.com/ruumble-project/ruumble-site-office/blob/main/docs/decisions/0029-high-availability.md): high availability, requirements and design
- [ADR-0030](https://github.com/ruumble-project/ruumble-site-office/blob/main/docs/decisions/0030-languages-and-split.md): Go, the split between foundation and service, Protobuf as the spec
- [`spec/protocol/`](https://github.com/ruumble-project/ruumble-site-office/tree/main/spec/protocol): the protocol definitions

| Term | Meaning |
|---|---|
| foundation | this component; binary `foundation` |
| node | one running instance; the foundation runs on three nodes |
| service | the Ruumble service from `ruumble-server`, one per node, talking to its local node |
