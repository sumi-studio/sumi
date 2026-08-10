# ADR 0007: Production runtime bootstrapの責務境界

- Status: Accepted
- Date: 2026-07-22
- Amends: [実装計画](../agent/implementation-plan.md) §1.2、§2.2、§3.4、§5.2、§8.1、§10.1〜§10.2、§11.1、§13、§14.1 と [T13/T13B/T15〜T17/T19〜T24/T26〜T29](../../apps/agent/TASKS.md)

## コンテキスト

従来のタスク分解はT15へM0 `main.rs`のecho差替えを要求した。一方、productionでSessionを
真に構築するには、T17が所有する認証済みidentity下のdurable transcript anchors、provider context、
Store state hydrationと全phaseの論理suffix recoveryに加えて、T21のThreeLayerMemory、T23のApprovalBroker、
T24のGateway接続fence、production ToolRegistry、executor/broker RPC境界が必要である。

T17はT16へ依存し、T16はT15へ依存するため、T15でproduction bootstrapまで完成させる要求は
依存循環になる。空history、空provider context、env/default identity、no-tool、fresh conversation限定で
差替えることは、既存conversationの継続性とgeneration fenceを偽るため受け入れられない。

## 決定

- T15は注入済みのSession/Run core、command stream、Gateway sink、executor境界に加え、既に実装済みの限定的な
  retry-wait control injection、idle/post-run Abort cutoff、boundedな注入control/cancellation/phase-observation seamsを所有する。
  T16はactive/live分類、run/provider/tool/approvalがactiveな間のcutoff、steer groupのsnapshot・一括注入、
  owner移譲、live control selectsを同じ注入runtime上で完成・受入する。T15の限定挙動だけをT16受入の代替にしない。live durable
  commit receiptが返す`message_id/message_seq`を`ContextMessage::Persisted` anchorへ保ち、決定論的な注入harness/E2Eで検証する。
- 完了済みT13はtools/executor境界までを正本とし、共有runtime contractを遡及追加しない。未完了のblocking backfill T13Bは、
  現行executor-local validator/identity usersを中立な`src/runtime/contracts.rs`へ移し、`ProcessGeneration`、`ProcessGenerationLease`、
  `GenerationRecoveryFence`、`RpcBootNonce`の値型とvalidatorを定義する。T17/T24はT26実装を待たずこの
  型を共有する。T13BはT15完了判定、T16、T17、T24、T26のblocking prerequisiteであり、allocator、issuance、production lease acquisitionを実装しない。
- T17は認証済みtenant/agent/conversation identity、validated `ProcessGeneration`、T13Bの共有型で表現され、
  productionではT26が取得・発行した`ProcessGenerationLease`と現世代exclusive ownershipを証明するtyped `GenerationRecoveryFence`を注入して
  Store scopeへ束縛し、persisted transcript anchors、provider context、Store上のmemory/command/phase stateを
  復号・検証する。typed `HydratedRunState`とphysical recovery intentsを返し、intentsが空なら同じfence内で
  全phaseの論理的な不足suffixを完了してstableな`HydrationReceiptIdentity`を持つhydration receiptを返す。非空ならT27が`receipt_id`、digest、
  `ProcessGeneration` lease、canonical exact intent setへ束縛して永続化した`PhysicalRecoveryReceipt`を再注入した後だけ影響suffixを完了する。
  T17はT27のphysical proof storeとは別のapplication ledgerへ、既存`tool_executions.tool_call_id` PKを
  canonical keyとするsorted unique exact intent set、logical suffix、
  該当する`indeterminate` terminalを同一transactionで記録する。同一receipt ID+digest+lease+canonical exact intent setの
  再送はledgerの全行完全一致時だけcrash後も`already-applied`としてidempotently受理し、stale、lease/generation・
  intent set不一致、conflicting receipt、reused ID with different digestは拒否する。各intentの`command_id`、
  `run_id`、`executor_generation`はidentityへ組み込まず、親tool executionのimmutable attestationとしてexact matchを要求する。
  ledger子は`PRIMARY KEY(receipt_id, tool_call_id)`、`UNIQUE(tool_call_id)`、T17 application親への`receipt_id` FK、
  `tool_call_id` FK、親子4列のcomposite FK/UNIQUEを持ち、同じexecutionの読み替えを拒否する。physical recovery対象の
  全`running` intentで`indeterminate_terminal_seq`を`NOT NULL UNIQUE`かつ`agent_events(seq)` FKとする。
  application親の`receipt_id`、receipt/intent-set digest、lease binding、generation、intent count、logical suffix先頭/末尾seqは
  すべて明示`NOT NULL`とし、`intent_count > 0`、`generation >= 0`、suffix先頭が非負、suffix末尾が先頭以上であることを
  CHECKする。generationは共有`ProcessGeneration` validatorでも`0..=i64::MAX`を強制し、suffix先頭/末尾はそれぞれ
  `agent_events(seq)` FKへ束縛する。子の`receipt_id/tool_call_id/command_id/run_id/
  executor_generation/indeterminate_terminal_seq`もすべて明示`NOT NULL`とする。全子INSERT後・commit前に親の
  `intent_count`と子行数のexact equalityを検証する。SQLiteのrowid `PRIMARY KEY`だけではNULLを拒否しないため、
  既存`tool_executions.tool_call_id`も`TEXT NOT NULL PRIMARY KEY`へ修正し、NULL直接INSERT拒否をmigration fixtureで固定する。
  EventWriterのtyped batch/schema validationは参照先が同じtool execution/receiptの正しい型の`indeterminate`
  terminal eventであることを検査する。さらにCOMMIT前に全first/last/terminal参照を実eventへ解決し、batchが発行する
  logical suffix eventの正規seq集合がfirst..=lastと完全一致することを検証する。cross-table/exact-membershipをSQLite
  `CHECK`で表現したとはみなさず、FK/CHECKとEventWriter validationを組み合わせる。ledger親・全子・logical suffix・terminal event/toolResultは同一transactionで
  全件なし/全件ありとし、orphan/null/wrong-event参照やterminalなしghost child reservationを許さない。
  既存`idempotency_key`とToolExecution Start/Finish APIは変更しない。完全な`RunCore`、
  T19〜T21のThreeLayerMemory、T23のApprovalBroker、production ToolRegistryを構築せず、物理kill/reapも
  実施済みと主張しない。決定論的テストは正しいproofを明示注入し、欠落・破損proofをfail-closedにする。
- T29は`conversation reset`を提供しない。[ADR 0008](0008-personality-agent-identity-and-execution-fabric.md)に従い、
  canonical life logの消去は人格agentのdeath/deletionであり、同じ`PersonalityAgentId`を継続・再利用して
  同じ本人が存続したことにはできない。後継は新しい`PersonalityAgentId`、DB、鍵、life log、VM、
  private work environmentを持つ別個体としてprovisionする。agent deathの開始は認証済みproduct/control-plane
  operationとagent-lifecycle domainが所有するcommit時認可を正本とし、
  [ADR 0013](0013-tool-invocation-routes-and-authority-provenance.md)の
  invocation route、AutoReview、Human approvalだけでこの認可を作成・拡張・迂回しない。
  T29のうちagent death実装は[#76](https://github.com/sumi-studio/sumi/issues/76)が担う。対象VM外の
  `deletion_tombstones`を正本として`requested → fenced → live_purged → backup_expired`を単調・冪等に進め、
  supervisorが一つのcurrent `ProcessGeneration`をfenceした後に、対象agentのDB/canonical life log、recovery
  ledger、event/execution、全key、private workspace、artifact volume、VM credential、backupを破棄する。
  shared Workspace data、外部tombstone/audit、他agentは存続する。同じWorkspace・administrative contextの
  第二agentを使うfixtureで対象だけが消え第二agentのprivate stateが不変であることを固定し、同一DBの
  「別conversation」やFK回避のconstraint無効化を隔離証拠にしない。この置換はT17/T27のphysical recovery
  proof、fail-closed、idempotence契約を弱めない。
- T24はproduction `GatewayConnector`/`ConnectionSupervisor`と認証済み接続fenceを所有する。
  `ConnectionEpoch`は再接続ごとに変わるT24-local identityであり、shared process identityではない。T24は各
  `ConnectionEpoch`についてtransport-neutral opaque `DeliveryEpoch`をexactly onceで1つmint/mapし、epoch終了時に対応mappingを
  exactly onceでinvalidateする。再接続後の旧DeliveryEpoch由来late frame/errorは拒否・dropして新epoch/cursorを変更しない。
  T17のDeliveryPumpは現在install済みのopaqueなDeliveryEpochだけを受け入れ、接続ライフサイクルidentityを構築・invalidate・stale判定しない。
- T26はT13B、T17、T21、T23、T24へ明示依存する。共有型を再定義せず、persistent monotonic allocator/
  issuanceとproduction lease acquisitionの唯一のownerとして
  runtime bootstrapより前に`ProcessGeneration` leaseを発行・永続化し、Gateway credential/hello、
  Store scope、Session、runtime/executor/artifact brokerへ配布する。T17のhydration結果、T21の
  ThreeLayerMemory/ContextAssembler、T23のApprovalBroker、production ToolRegistry、provider、Gateway、
  executor境界を唯一のproduction `RunCore`へ構成し、M0 echo shellをproduction `main`へ差し替える。
  `apps/agent/src/main.rs`と明示的なbootstrap composition moduleを所有する。executor/broker RPC専用の
  `RpcBootNonce`は同じ`ProcessGeneration`と対にし、`ConnectionEpoch`の代用にしない。
  T17のphysical recovery intentsが空のclean existing conversationはT27を待たず構成できる。非空なら
  hydration receiptを得られずReadyをlatchせず、T27 integrationまでfail-closedにする。
- `ProcessGeneration`のdomainは`0..=i64::MAX`で、0も有効である。T26 allocatorはincrement前に最大値を
  検査し、`i64::MAX`後はwrap/reuseせず新bootstrapをfail-closedに拒否する。全componentへのdistributionと
  mismatchをテストする。
- T27はT17が返した非空physical recovery intentsとT26が発行したleaseを消費し、旧generationのkill/reap、
  resource quota、execution registry、descendant cleanup、crash recoveryを実行する。完了したintent set・lease・
  `ProcessGeneration`へ束縛した`PhysicalRecoveryReceipt`を`receipt_id`+digest付きで永続化してT17へ返し、`tool_call_id` canonical keyと親行attestationが同一の
  再送だけを`already-applied`として受理する。stale、lease/generation・intent set不一致、conflicting receipt、reused ID with
  different digestは拒否し、T17検証後にT17 application ledgerと同一transactionで`running → indeterminate`を完了する。
  T27はphysical proof persistenceを所有するが、T17 application ledgerを所有・代替しない。
  allocator/issuanceは重複実装せず、空intentsのT26 bootstrapを妨げない。
- `HydrationReady`はedge signalではなく`ProcessGeneration`ごとのlatched stateである。current generationは必ず
  `NotReady`から始まり、T17のstable `HydrationReceiptIdentity`へ束縛されたimmutableな
  `Ready { generation, hydration_receipt_identity }`へ一度だけ遷移する。T26はgeneration rollover時に旧Readyを
  新generation公開前または同じatomic state transitionでinvalidateし、新generationをNotReadyから始める。
  旧generationのlate Ready、generation不一致、同generationで別receipt identityへの再latchは拒否する。
  各T24 ConnectionEpochはhello成功後にedgeを待たずcurrent stateを観測する。NotReadyならcommandをbounded
  hold/backpressureし、上限超過時は接続をfail-closedに閉じる。ready前はcommandをSessionへ公開せず、
  ACK、provider call、executor RPCを開始しない。fixture ownershipはT24がready-before-hello/hello-before-ready、
  T26がrollover invalidation/stale旧generation拒否、T28がproduction ready-after-reconnectを担当し重複させない。
- M0 admission echo shellはT26まで実行可能に保つが、T15/T16完了やCloud releaseの証拠に数えない。
  stdioはM1以降も完全な依存を注入したlocal harness/E2Eとしてだけ使い、production bootstrapを代替しない。

production bootstrapではidentityをenv/defaultから補わず、既存conversationの鍵、row、anchor、復号、
provider contextの不整合を空contextとして継続しない。tool registryを空にするfallbackやfresh-only制限も
置かず、Session/provider/executor開始前にfail-closedとする。

## 根拠と失敗条件

依存は偽の線形鎖ではなく、次のbranchとconvergenceを持つ。

```text
T13 → T13B shared runtime contracts → T15 completion → T16 → T17 → T18 → T24 ─┐
                                                       ├→ T22 → T23 ───────────┤
T17 → T20 ← T19; T20 → T21 ────────────────────────────────────────────────────┤
T13B shared runtime contracts ──────────────────────────────────────────────────┤
                                                                                └→ T26 ┬→ T27
                                                                                        └→ T28 ← T18/T24
```

これはマイルストーン完了順を偽の線形な実装DAGへ変換しない。T19の純関数部はT17と並列で、T20がT17/T19を収束しT21へ進む。approval branchはT16からT22/T23へ進み、T22/T23とT24はM4完了を待たず実依存だけで進める。
T26はT13B/T17/T21/T23/T24を直接入力として唯一のproduction compositionを行う。これによりT17が
将来componentを先取りして偽の`RunCore`を返すことなく、coreの早期検証とproduction構築の真実性を両立する。

次は契約違反である:

- durable commit前のID/seq、配列位置、合成IDを`Persisted` anchorに使う。
- 認証identity/`ProcessGeneration`とGateway credential、Store scope、Session、runtime/executor/broker RPCの
  generationが一致しない、または`RpcBootNonce`が同generationと対にならないまま開始する。
- 既存conversationのhistory/provider context読出し失敗を空としてproviderへ送る。
- lease/exclusive fenceが欠落・破損している、または非空intentsに必要な`PhysicalRecoveryReceipt`/T17 application ledgerが欠落・破損・
  stale・別intent set/generation・conflicting receiptなのに論理復旧またはphysical cleanup済みとして進む。同一receipt
  IDの異なるdigest再利用も拒否し、負またはdomain外generation、負・逆転・danglingなlogical suffix境界、
  exact suffix membership不一致も拒否する。`after_t27_receipt_persist`、`before_t17_logical_suffix_transaction`、
  `after_t17_logical_suffix_transaction` failpoint後にapplication ledger親・全子、logical suffix、`indeterminate` terminalを全件なし/全件ありにし、二重生成しない。orphan `receipt_id`、null terminal seq、通常eventまたは別tool/receipt terminalへのwrong-event参照、terminalなしghost child reservationもschema/EventWriter fixtureで拒否する。前者はT27 integration/Cloud global acceptance、後二者は明示注入receiptを使うT17単体acceptanceのownerとし、T17完了をT27へ循環依存させない。
- current generationがNotReady、stale generationのReady、またはreceipt identity不一致なのにcommandをSessionへ
  公開し、ACK/provider/executorを開始する。helloより先のReadyをedgeとして失うことも違反である。
- M0 echo、注入mock、fresh conversationだけの成功をproduction bootstrapの証拠にする。
- stdio local harnessをproduction bootstrapまたはCloud WSの証拠にする。
- T26のallocator/issuanceをT27へ重複実装する、またはT27のphysical recoveryをT26へ移す。

## 棄却した代替

- **T15でproduction `main`を差し替える**: T17/T24/T26の成果を先取りし、依存循環または偽bootstrapになる。
- **T15で暫定empty/fresh/no-tool runtimeを起動する**: 同じCloud release内に二重仕様を作り、既存会話の
  継続とtool境界を検証しないまま成功扱いにする。
- **T17で`RunCore`または`main`まで組み立てる**: T19〜T23の完成前にmemory/approval/tool境界を偽り、
  Gateway lifecycleとexecutor isolationをStoreへ漏らしてT24/T26のownershipを崩す。
- **T26をT24だけへ依存させる**: hydration、ThreeLayerMemory、ApprovalBroker、ToolRegistryがT26の直接入力である
  受入契約を隠し、接続だけでempty Sessionを開始する余地を残す。
- **allocatorをT27へ置く**: production bootstrapは有効なprocess leaseより後でなければならず、T26がT27を
  待つ循環かleaseなしbootstrapになる。persistent monotonic allocator/issuanceはT26、leaseを使う物理復旧は
  T27へ分ける。
- **physical recoveryをT26へ移す**: quota、execution registry、descendant cleanup、crash recoveryをT27から
  分断し、cleanup proofのownerを曖昧にする。

## 影響と下流境界

T15/T16のコードPRはproduction `main`を触らず、注入harnessでcore gateを閉じられる。T17は単なるStore
拡張ではなくtyped boot hydration API、physical recovery intents、T27 proofとは別のdurable application ledgerを受入対象にする。
T24はGateway/connection supervisorとexactly-once `ConnectionEpoch`/`DeliveryEpoch` mapping/invalidation、および旧epochのlate
frame/error拒否のownerのまま、T26が`ProcessGeneration`を発行して
空intentsのclean existing conversationを含むproduction runtimeを構成する。T27は非空intentsに対し、そのleaseを
使うquota/reaper/fault-injectionと永続化済みidempotent `PhysicalRecoveryReceipt`を追加する。T28のWS
production E2EはT26 bootstrapとhello→`HydrationReady` gateを前提にする。pre-launchのため旧bootstrapとの
compat経路は作らない。
