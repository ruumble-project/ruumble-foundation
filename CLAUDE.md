# CLAUDE.md: ruumble-foundation

This repo is `ruumble-project/ruumble-foundation`, Ruumble Foundation: the clustered voice server in Go (ADR-0029 and
ADR-0030 in `ruumble-project/ruumble-site-office`). Read `README.md` first.

## Rules

- **The spec lives in the site office.** Protocol definitions come from `spec/protocol/` there; never change a copy
  here by hand.
- **Written from the spec, not from a model.** The foundation is written from Mumble's protocol definitions and
  documentation, not from an existing third-party server implementation (ADR-0030).
- **Nothing here refers to the site office's checkout.** The site office pins this repo, never the other way round.

## Team and GitHub

The team rules of the server apply here unchanged: section "Team and GitHub" in `CLAUDE.md` of
`ruumble-project/ruumble-server` and its `docs/workflow.md` (ADR-0026). In short: English everywhere, no em or en
dashes, commit messages one line without trailers, the team pushes to `main` directly after the checks.
