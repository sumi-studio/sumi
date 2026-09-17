# Alpha acceptance matrix

Status of each Description claim as of the 2026-09-18 acceptance round. This
distinguishes what was *exercised against the real placement* from what merely
*exists in source*, what was *mocked*, and what remains untested. Sources:
the Local distribution run under `releases/2026-09-18/alpha-distribution-acceptance`
and the root-reviewed Cloud acceptance under
`releases/2026-09-18/{files-and-transfer-public-deploy,public-transfer-execution}`.

## Local host (sumi-local)

| Capability | State | Evidence / limit |
|---|---|---|
| Pack (`sumi-local pack`) | exercised | Builds a portable tarball (prebuilt linux/amd64 binary + JS host + CLI + compose + docs) on the host toolchain |
| Install as recipient (`install --db-url`) | exercised | Real pack installed in a container with Node + utilities but **no Go, no source repo, no module cache**; shim + config + state under fixture `/root` |
| Install with `--managed-pg` | untested locally | This run used an external DB URL; managed-PG execution was blocked by fixture network allocation. Existing CI (`agent-core.yml`) runs the shipped self-tests; a successful, non-skipped ownership step on the integrated source is still required |
| `doctor` / `start` / `status` / `say` / `url` / `logs` / `stop` | exercised | All verified against the real install |
| Browser engineering page | exercised | Headless Chromium in the fixture: real form submit → reply rendered, history listed, zero console errors |
| Identity continuity (stop/start, uninstall/reinstall, upgrade, container recreate) | exercised | Same persona id + full outbox history in every case |
| Crash recovery (SIGKILL mid-turn) | exercised | Dead lease waited out (~30 s), turn re-driven, exactly one `turn_completed` for the tested mock-provider turn |
| Scheduled wake, `openai-stub` provider, pack-reinstall, `--purge` | exercised | Via shipped `self-test.sh` run inside the fixture (ALL CHECKS PASSED) |
| Real model provider | mocked | All runs used the deterministic `mock` (`echo:`) or `openai-stub` provider. Not a real secretary reply |
| Platforms other than linux/amd64 | untested | Fixture is linux/amd64 only; macOS/native Windows/ARM unverified |

## Cloud placement and transfer

| Capability | State | Evidence / limit |
|---|---|---|
| Deployed alpha (API + Web + new secretary core) | exercised | Live at the accepted `6a58a6cc` source; health + hosted app checked read-only |
| Local→Cloud secretary transfer (state-only) | exercised once | Happy + cancel paths verified against live Cloud: same persona activated, carried input present, Cloud `201` / Local `410 secretary_moved`, cancel restored Local. Person-facing UI **is implemented** (secretary-move panel on the sign-in screen) but was not exercised end to end in this round |
| Invitation issuance | guarded CLI | The invitation was seeded through a command-line path; the public consumption, auth-flow confirmation, transfer and post-move requests ran on real HTTP endpoints |
| Cloud admission of a post-move message via DirectChat | exercised (API level) | HTTP `201` = admission/projection of the submitted input — **not** a secretary answer; no model ran and no browser drove it |
| External IdP sign-in proof for transfer | synthetic | Substituted for the test; a real IdP session was not part of the evidence |
| Public file operations (F343) | partially exercised, blocked | Initial write/read passed on real Cloud storage; subsequent conditional update and cleanup returned `503 pending_settlement`. Core-driven operations were not reached |
| Cloud→Local transfer | implementation underway | Common portable return work is in progress; no accepted return-flow evidence yet |
| Product-level daily use (real person's sign-in + real model) | untested this round | Real model/IdP/browser flows were not exercised in this round at this source. The deployed Core's fallback provider is `none` — an unconfigured default, not evidence about a person's selected connection |

## Gaps to close before Alpha release

1. **Managed-PG install path** — not verified in this local run; the existing
   `agent-core.yml` CI job runs the self-tests that cover it, so an integrated
   change gains that evidence on its next CI pass.
2. **Real login + real model on the deployed app** — not exercised in this
   round at this source; substitute proof was used for IdP.
3. **F343 public file access** — other owner, `pending_settlement`.
4. **Sustained use** — this round did not perform a long-running Local or
   hosted-app soak.

The recipient evidence is limited to linux/amd64, with an external database
and mock or stub model providers. Other platforms have not been verified in
this round. The conflicting `say --id` response is fixed to HTTP 409 in this
candidate, pending integration. `pack` still requires its output directory to
exist; this is a current usage constraint, not a new release requirement.
