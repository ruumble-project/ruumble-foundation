# Ruumble Foundation

What the Ruumble building stands on: a clustered voice server in Go that replaces the Mumble core underneath.
It runs on three nodes, speaks Mumble's protocol (clients from Mumble 1.5 on), forwards voice between nodes, and
keeps all stored state replicated through Raft, so a node can fail or be updated without the building going quiet.

What there is so far: nodes that serve Mumble clients with one channel and voice over the TCP tunnel, and the
interconnect between them (ADR-0032): every node shows the users of the others and forwards voice to them, so two
clients on different nodes hear each other. Raft, stored state, channels and UDP follow. The design lives in the site office,
[`ruumble-project/ruumble-site-office`](https://github.com/ruumble-project/ruumble-site-office):

- [ADR-0029](https://github.com/ruumble-project/ruumble-site-office/blob/main/docs/decisions/0029-high-availability.md): high availability, requirements and design
- [ADR-0030](https://github.com/ruumble-project/ruumble-site-office/blob/main/docs/decisions/0030-languages-and-split.md): Go, the split between foundation and service, Protobuf as the spec
- [ADR-0032](https://github.com/ruumble-project/ruumble-site-office/blob/main/docs/decisions/0032-node-interconnect.md): the interconnect between nodes over QUIC
- [`spec/protocol/`](https://github.com/ruumble-project/ruumble-site-office/tree/main/spec/protocol): the protocol definitions

| Term | Meaning |
|---|---|
| foundation | this component; binary `foundation` |
| node | one running instance; the foundation runs on three nodes |
| service | the Ruumble service from `ruumble-server`, one per node, talking to its local node |

## Running

```sh
go run ./cmd/foundation -listen :64738         # a node on its own; a throwaway certificate is made at start
go test -race ./...
docker build -t ruumble-foundation .
```

A cluster needs a key per node for the interconnect and a cluster file that lists every node with the pin of its key:

```sh
foundation keygen -cert fsn1.pem -key fsn1.key -name fsn1   # prints the pin
foundation pin fsn1.pem                                     # prints it again
foundation -cluster cluster.json -node 1 -node-cert fsn1.pem -node-key fsn1.key
```

```json
{"cluster": "prod", "nodes": [
  {"number": 1, "name": "fsn1", "addr": "10.0.0.1:64739", "pin": "..."},
  {"number": 2, "name": "fsn2", "addr": "10.0.0.2:64739", "pin": "..."},
  {"number": 3, "name": "nbg1", "addr": "10.0.0.3:64739", "pin": "..."}]}
```

The interconnect listens on UDP at the port of the node's own `addr` (or `-mesh-listen`); only the nodes need to
reach it. The node with the lower number dials. Node keys are trusted by their pin, not by a certificate authority,
and `keygen` makes them valid for a hundred years.

## Layout

| Path | What |
|---|---|
| `cmd/foundation` | The node binary |
| `internal/voice` | The side Mumble clients talk to: handshake, users, voice forwarding; users and voice of the other nodes |
| `internal/mesh` | The interconnect: QUIC between nodes, pinned node keys, live state on a control stream, voice and pings as datagrams |
| `internal/wire` | Framing of Mumble's control channel |
| `internal/certs` | TLS certificate, or a throwaway one |
| `proto/` | Copies of the protocol definitions from the site office's `spec/protocol/`; never edited here |
| `gen/` | Go code generated from `proto/` by `buf generate`, committed |
