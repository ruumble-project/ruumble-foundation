# proto

Copies of the protocol definitions from `spec/protocol/` in `ruumble-project/ruumble-site-office`, synced by its
`tools/sync-spec.sh`. Never edit them here; change the spec there and sync. `buf generate` turns them into `gen/`.

| Path | What |
|---|---|
| `mumble/` | Mumble's own definitions, unchanged, under Mumble's license (`mumble/LICENSE`) |
| `ruumble/node/v1/` | The interconnect between nodes (ADR-0032) |
| `ruumble/foundation/v1/` | The foundation API (ADR-0031); here only what the interconnect imports |
