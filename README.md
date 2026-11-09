# Ruumble Foundation

What the Ruumble building stands on: a clustered voice server in Go that replaces the Mumble core underneath.
It runs on three nodes, speaks Mumble's protocol (clients from Mumble 1.5 on), forwards voice between nodes, and
keeps all stored state replicated through Raft, so a node can fail or be updated without the building going quiet.

What there is so far: a node that serves Mumble clients on its own, with one channel and voice over the TCP
tunnel. Clustering, UDP and stored state follow. The design lives in the site office,
[`ruumble-project/ruumble-site-office`](https://github.com/ruumble-project/ruumble-site-office):

- [ADR-0029](https://github.com/ruumble-project/ruumble-site-office/blob/main/docs/decisions/0029-high-availability.md): high availability, requirements and design
- [ADR-0030](https://github.com/ruumble-project/ruumble-site-office/blob/main/docs/decisions/0030-languages-and-split.md): Go, the split between foundation and service, Protobuf as the spec
- [`spec/protocol/`](https://github.com/ruumble-project/ruumble-site-office/tree/main/spec/protocol): the protocol definitions

| Term | Meaning |
|---|---|
| foundation | this component; binary `foundation` |
| node | one running instance; the foundation runs on three nodes |
| service | the Ruumble service from `ruumble-server`, one per node, talking to its local node |

## Running

```sh
go run ./cmd/foundation -listen :64738         # a throwaway certificate is made at start
go test -race ./...
docker build -t ruumble-foundation .
```

## Layout

| Path | What |
|---|---|
| `cmd/foundation` | The node binary |
| `internal/voice` | The side Mumble clients talk to: handshake, users, voice forwarding |
| `internal/wire` | Framing of Mumble's control channel |
| `internal/certs` | TLS certificate, or a throwaway one |
| `proto/` | Copies of the protocol definitions from the site office's `spec/protocol/`; never edited here |
| `gen/` | Go code generated from `proto/` by `buf generate`, committed |
