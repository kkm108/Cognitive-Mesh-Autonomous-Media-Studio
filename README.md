# Cognitive Mesh — Autonomous Media Studio

A decentralized, **zero-orchestrator** agentic mesh that turns a seed topic into a
published short-form video with no central coordinator, no static manifest, and no
hard-coded pipeline.

Nodes discover work by subscribing to events on a PubSub bus, decide their own
model tier through dynamic routing, and publish downstream events. Any node can be
replaced at runtime (hot-swap) without touching the rest of the mesh.

See **[ARCHITECTURE.md](ARCHITECTURE.md)** for the full design.

---

## Quick start

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass -Force

npm install
npm run verify          # end-to-end: topic -> research -> ... -> pipeline.complete
npm run verify:mesh     # bus, routing, supervisor, hot-swap checks
npm run typecheck
npm start               # run the mesh
```

On first run, copy `.env.example` to `.env` and fill in credentials. Without them
the mesh runs in degraded mode — MCP nodes synthesize fallback data so the
pipeline still completes and stays verifiable.

---

## Scripts

| Script | Purpose |
| --- | --- |
| `npm start` | Boot the mesh (`mesh/bootstrap.ts`) |
| `npm run dev` | Same, with file watching |
| `npm run verify` | Full autonomy check, topic → published |
| `npm run verify:mesh` | 5-stage mesh integrity check |
| `npm run typecheck` | `tsc --noEmit` |

---

## Batch helpers

### `update.bat` — commit and push changes

```bat
update.bat                         rem auto-message + push to origin
update.bat "Added visual node"     rem custom message
update.bat "Added docs" origin     rem push to a specific remote
update.bat --all                   rem push to EVERY configured remote
update.bat --pull                  rem pull --rebase first, then commit + push
```

Files are staged automatically (`git add -A`). If the tree is clean it still pushes
any existing commits. If the remote rejects the push, a `git pull --rebase` is
attempted before retrying.

### `push-new.bat` — add a repository URL and push

```bat
push-new.bat https://github.com/owner/repo.git
push-new.bat owner/repo
push-new.bat new-repo "Added docs"
push-new.bat --bulk repos.txt
```

The remote is added if missing, or its URL is updated if present. If the same URL
is already registered under a different name, that name is reused instead of
creating a duplicate.

For bulk use, copy `repos.txt.example` to `repos.txt`, list one repo per line:

```bat
set GH_OWNER=your-user
push-new.bat --bulk repos.txt
```

---

## Layout

```
mesh/       event bus, schema, supervisor, persistence, bootstrap
nodes/      research, script, visual, assembly, publish, template
router/     dynamic model routing (frontier vs fast tier)
mcp/        MCP-native tooling layer — no scrapers allowed
```

**MCP only.** Playwright/Selenium scraping is not permitted; every external
capability is exposed through an MCP server.
