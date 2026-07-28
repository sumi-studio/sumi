# ADR 0008: 人格agentのidentity・所有境界・VM内execution fabric

- Status: Proposed
- Date: 2026-07-28
- Amends:
  - [ADR 0002](0002-agent-stack.md)
  - [ADR 0004](0004-agent-local-platform-support.md)
  - [ADR 0007](0007-production-runtime-bootstrap-boundary.md)
  - [エージェントワークスペース設計](../agent/workspace.md)
  - [3層メモリ設計](../agent/memory.md)
  - [エージェント実装計画](../agent/implementation-plan.md)
  - [実装タスク](../../apps/agent/TASKS.md) T26〜T29
- Related:
  - [#74](https://github.com/sumi-studio/sumi/issues/74)
  - [#75](https://github.com/sumi-studio/sumi/issues/75)
  - [#76](https://github.com/sumi-studio/sumi/issues/76)
  - [#77](https://github.com/sumi-studio/sumi/issues/77)
  - [#78](https://github.com/sumi-studio/sumi/issues/78)
  - [#79](https://github.com/sumi-studio/sumi/issues/79)
  - [#80](https://github.com/sumi-studio/sumi/issues/80)
  - [#81](https://github.com/sumi-studio/sumi/issues/81)
  - [#82](https://github.com/sumi-studio/sumi/issues/82)

## Context

Sumiは、複数の人間と複数の人格を持つAI secretaryが、同じWorkspaceで
時間を共にし、会話・タスク・予定・文書・メール・ブラウジング・会議・
学習・その他のアプリを使って生活し、協働するプロダクトである。

Sumiのproduct semanticsでは、人格agentを単なるruntime、controller、
automation moduleとして扱わない。一人の生活・経験・判断・関係を持つ主体
として、人間と同型に扱う。private Agent VMはそのagent本人のPCであり、
toolやAI harnessは本人が意思を持って利用する道具である。

今回実装しているagent foundationは、その共有Workspace全体ではない。
一体の人格agentに閉じた、人格・人生ログ・記憶・provider・tool・private
work environment・process lifecycleを動かす基盤である。共有Workspaceの
domain dataとcoordinationは、この基盤の外側でAPI/control planeを通して
実装する。

現行正典と実装には、この二つの境界を後から接続しにくくする前提がある。

- `tenant`、人間、人格agent、VM、Linux workspaceのowner表現が混在する。
- `agent_id`と`conversation_id`が一対一で存在する一方、
  `conversation_id`だけを交換して人生ログを消去するresetを想定する。
- ブラウザ/APIは、人間が人格agent本人ではなく交換可能なconversationへ
  話しかける形で公開contractを持つ。
- Cloud配置をtenantごとのmicroVMとし、同一Workspaceにいる複数agentの
  private PCとfailure domainを表現できない。
- 親agentの直接tool実行はあるが、人格なしsubagentのidentity、権限委譲、
  寿命、結果回収、回復、監査の境界がない。
- shellは非対話の単発`bash -c`のみで、stdin、PTY、resize、attach、
  持続terminal identityを持たない。
- runtime generationとexecutor/broker RPC process epochのidentityはあるが、
  直接agentとsubagentを区別するper-call authorityはない。

これらをすべて今回のreleaseで実装することは本ADRの目的ではない。
目的は、人格agentのidentityと所有境界を確定し、T26以降が将来のSumiを
閉ざす不可逆な構造を作らないことにある。

## Decision

### 1. Product actorとcontainment

正本のcontainmentは次とする。

```text
Sumi Workspace
├── Human member 0..N
├── PersonalityAgentId 0..N
│   ├── one continuous agent session / life log
│   ├── one direct-chat surface
│   ├── one private Agent VM / private work environment
│   └── one VM execution fabric
│       ├── PersonalityAgent principal
│       │   ├── direct tool execution 0..N
│       │   └── TerminalSession 0..N
│       └── SubagentInvocationId 0..N
│           ├── delegated tool execution 0..N
│           └── TerminalSession 0..N
└── shared conversations, tasks, calendars, documents, apps and permissions
```

`Sumi Workspace`は共有のproduct/domain resourceである。各人格agentの
private VMはWorkspaceそのものではなく、agentの私物PCに相当する。

人格agent同士の協働は同じVMへ入ることではなく、Workspace上の共有resource、
会話、task、権限付きaction、明示的なdelegationを通して行う。

既存control planeの`tenant_id`はadministrative/security scopeを表す
identityとして扱い、Sumi Workspace、agent、VM、Linux `/workspace`の別名には
しない。tenantとWorkspaceのcardinalityは本ADRでは決めない。少なくとも
`tenant_id`が同じであることを、同じVM・volume・private work environmentを
共有する根拠には使わない。

### 2. PersonalityAgentIdと人生ログは同じ寿命を持つ

`PersonalityAgentId`を人格を持つ持続的なproduct actorの正本identityとする。

人格agentには一つの連続したagent sessionがある。そのsessionで経験した
direct chat、Workspace由来の出来事、判断、actionがagentの人生ログになる。
初期のfrontend chatは内部ログviewerではなく、人間がそのagent本人へ直接
話しかける可視の正面入口である。

人格agentはWorkspace内の場所ごとに別sessionを持たない。後続のtask、
mail、calendar、app等は、source/resource/actor/correlation metadataを伴って
同じagent sessionへ入り、同じ人格agentが各surfaceへ作用する。

公開contractに独立した交換可能な`ConversationId`を持たせない。
ブラウザ、Gateway、command、event、token、artifact namespaceは
`PersonalityAgentId`を宛先・ownerとして使う。

後方互換性を持たないpre-launch contract replacementとして、SQLite、AAD、
RPC、token、route、fixtureから`conversation_id` fieldと独立conversation scopeを
削除する。
dual-read、dual-write、exact alias、旧AADのdecrypt/re-encrypt移行は実装しない。
pre-launchのDB・鍵・wire fixtureは`PersonalityAgentId`だけを使って再生成する。

### 3. 破壊的conversation resetは存在しない

人生ログは人格の構成要素である。暗号化されたraw transcript/event historyを
正本とするcanonical life logを消去した後も、同じ人格agentが継続すると
扱ってはならない。

したがって、同じ`PersonalityAgentId`を残したままconversation IDを交換し、
transcript、memory、provider context、artifact、鍵を全消去する
`conversation reset`は廃止する。

canonical life logの消去は人格agentのdeath/deletionである。後継を作る場合は
新しい`PersonalityAgentId`、DB、鍵、人生ログ、VM、private work environmentを
持つ別個体としてprovisionする。

key rotationは履歴を失わないrewrap/re-encryptionである。crypto-eraseを
rotationと呼ばない。

canonical life logと、その派生物は同じretention contractではない。L0/L1/L2
memoryは正本履歴から作る置換可能なprojectionであり、compactionしてよい。
provider固有のopaque contextはprovider replay contractとanchorに従って
置換・crypto-eraseしてよい。redacted projectionと検索indexは正本から再構築
できる。tool-output artifact payloadはbest-effortであり、bounded quotaの
high/low watermark GC、個別retention、明示的tombstoneに従って回収できる。
artifactがGCされた事実と、人生ログ中のtool action/result referenceまで
消すことは同じではない。

このGC判断をinput attachmentや他のartifact classへ暗黙に拡張しない。少なくとも
active L0/provider inputから参照されるattachment payloadは、再開に必要な間は
pinする。attachmentの再生成可能性、retention、tombstoneは各artifact classの
既存contractまたは別Issue/ADRで定める。

Workspace上のview/resourceを閉じる、表示を整理する、共有resourceをその
lifecycleに従って削除することは、人格agentの人生ログを消去することとは
別である。選択的な忘却・redaction・法的retentionのproduct semanticsは
別Issue/ADRで定め、同一人格が無影響で継続したと暗黙に扱わない。

### 4. VMとprivate work environmentは人格agentが所有する

Cloudのphysical deployment unitはtenantでも人間でもconversationでもなく、
一体の`PersonalityAgentId`である。

各人格agentは一つのdedicated VM/private PCを持つ。VM内にruntime、
executor、artifact broker、private DB、private work environment、IPC、
process generation、sandbox、直接実行とsubagent実行を置く。

compute generationのrecycleやsleepは人格agentのdeathではない。永続状態を
復元し、同じ`PersonalityAgentId`と人生ログを継続する。

agent deletionはVM、DB、agent key、private work environment、artifact
volume、credential、backupをそのlifecycleに従って破棄する。一方、
Workspaceの共有domain data、他のagent、外部tombstone/auditは存続する。

control-plane policyは各agent VMの上限や許可を統治できるが、agent-local
quotaをadministrative scope全体のphysical resourceと呼ばない。aggregate
quotaはcontrol planeが複数agent deploymentを横断してmeter/enforceする。

### 5. 人格agentは人間と同じようにAI harnessを使う

実行主体を次のsum typeとして扱う。

```text
ExecutionPrincipal =
  PersonalityAgent(PersonalityAgentId)
  | Subagent(SubagentInvocationId)
```

`PersonalityAgent` principalは現在どおりtoolとshellを直接利用する。subagentを
必須proxyにしない。

`SubagentInvocationId`は親agentが自分のVM内で任意に作る人格なしのbounded
AI interactionである。別の人格agent、Workspace member、public address、
direct-chat surface、continuous agent session、canonical life log、private
VMを与えない。

その利用形態は、人間が自分のPCでCodexやClaude CodeのようなAI harnessを
使う場合と同型にする。人格agentは自然言語で依頼を始め、追加指示を送り、
subagentからの質問へ答え、進捗を見て、待機・中断・再開・cancelし、成果を
受け取って評価できる。staticなone-shot task packetを最初に渡して終わる
dispatch modelを正本にしない。

subagentは、interaction transcript、人格agentが共有したreference/attachment、
および許可された親VMをtoolで探索して自ら得た観察から、作業に必要なcontextを
発見・更新する。必要なcontextを起動前に列挙したprojectionへ限定しない。
一方、親のpersonality prompt、全人生ログ、private reasoning、sibling stateを
暗黙に複製してsubagentを親の分身にもしない。

context探索と作業方法はagenticでよい。決定論的に閉じるのはauthority、resource
scope、budget、寿命、cancel、auditである。subagentを使う判断、成果の評価、
その成果をSumi Workspaceへ作用させる責任は人格agentに残る。

invocation recordとruntime attemptを分ける。invocationは親子lineage、目的、
interaction transcript、共有reference、権限、budget、寿命、cancel cause、
outputs/evidence、terminal lifecycle stateを持つ。runtime attemptはrecycle
できる。

subagentの完全実装は段階導入してよいが、T26のbootstrap/tool boundaryは
親直接実行を維持したままVM-local execution managerを差し込めるinterfaceを
持たなければならない。

### 6. Execution authorityはruntime/RPC lifecycle identityと分離する

`ProcessGeneration`は人格agentのruntime/deployment generationを識別し、同じ
VM内のruntime recycleでも前進して旧generationをfenceできる。
`RpcBootNonce`は、そのgeneration内で起動した具体的なexecutor/broker RPC
process epochを識別する。VM boot、runtime generation、RPC process bootを
同じ寿命として扱わない。いずれも個別tool callのauthorityではない。

direct agent callとsubagent callの各effectは、exact
`ExecutionPrincipal`、owning `PersonalityAgentId`、canonical action digest、
resource/permission scope、audience、generation、expiry/revocation、
idempotency identityへ束縛したcall authorityを持つ。

subagentのeffective authorityは、少なくとも親のlive authority、platform/
Workspace policy、invocationで明示したcapabilityの積集合以下とする。childが
権限を製造・拡大できない。

人間、reviewer、policy engineはdecision actor/sourceであり、executor principal
ではない。Gatewayで認証したWorkspace actor、decision source、execution
principal、delegation lineage、exact call、outcomeをEventWriterのdurable
auditへ残す。client payload内のactor名を認証事実として扱わない。

### 7. 人格agentは人間と同じようにterminalを使う

private Agent VMは人格agent本人のPCであるため、人格agentはsubagentを介さず
terminalを直接使える。複数のterminalを開き、出力を観察して次の入力を考え、
model turnをまたいでstdin、resize、signal、detach/reattach、終了を行う。
terminalは一回のtool callに従属するcommand resultではなく、本人が時間の中で
操作する持続的な道具である。

人間がterminalを使う前に全command列、stdin列、期待outputを宣言しないのと
同じく、predeclared command scriptやone-shot execution packetを正本にしない。
観察したoutputに応じて次の入力、待機、signal、別terminalでの作業を選ぶ連続した
interactionとして扱う。

人格なしsubagentも、人間が使うAI harnessがその人のPCでshellを使うのと同じく、
委譲されたauthorityの範囲で自分のterminalを持てる。親の人格agentがterminalを
使うためにsubagentを作る必要はない。

現行の非対話`bash -c`は削除せず、同じexecution fabric上のpipe-backed、
closed-stdinなephemeral-command adapterとして維持する。

VM-local execution managerは次を持つ。

```text
TerminalKey =
  (ExecutionPrincipal, TerminalSessionId, ProcessGeneration)
```

- explicit `pty` / `pipes` mode
- stdin / EOF
- PTY resize
- signal
- attach / detach
- monotonic output cursorとbounded buffer
- per-terminal writer lease
- terminal単位、invocation単位、generation単位のcancel/reap

terminalのproduct ownerはexact `ExecutionPrincipal`であり、physical process
lifecycleはagent VM execution fabricが管理する。tool call、model turn、
WebSocket、UI connectionはownerではない。transport disconnectはdetachであり、
明示的なterminal terminationと同一視しない。

owner、authority、resource limit、lifecycle、auditは決定論的に強制する。
terminal内で何を試し、出力をどう読み、次に何を入力するかは人格agentまたは
subagentのagenticな判断へ任せる。

processはgenerationを越えて生存しない。persistent filesystemと人生ログは
継続するが、live terminalはgeneration rollover時にtyped terminal stateへ
閉じてreapする。

### 8. Shared Workspace由来のprovenanceを人生ログへ残す

同じ人格agentへ複数の人間・surface・resourceから入力が来るため、commandの
message contentだけを人生ログへ保存してはならない。

API/control planeが認証したWorkspace、actor、source surface、resource、
correlation、causationを、caller-asserted contentとは別のimmutable metadataと
してdurable command/eventへ束縛する。

共有domain dataの正本はWorkspace API側に置き、agent DBへ複製しない。
agent DBには、そのagentが何を経験し何をしたかを理解・監査・回復するための
identity、reference、projection、resultだけを保存する。

## Migration

1. 本ADRをreviewし、StatusをAcceptedへ変更する。
2. #74でpublic/internal `conversation_id`と独立conversation scopeを
   `PersonalityAgentId`へ統合し、破壊的resetをcanon、contract、T29から除く。
3. 後方互換のschema、alias、dual-read、data migrationを作らず、pre-launchの
   DB、鍵、AAD、wire fixtureを新identity contractで再生成する。
4. #75でper-agent deployment namespaceと、同じWorkspaceかつ同じ
   administrative scopeに属する二agent間のisolationをT26へ固定する。
5. #79、#80のendpoint/credential/readinessをT26へ組み込み、現在の直接agent
   verticalを完成させる。
6. #77、#81、#82を段階導入し、parent direct pathを常に回帰fixtureで守る。
7. #76でT29をagent-death lifecycleとして再設計する。

## Consequences for current tasks

### T26 / PR #49

- PR #49はquarryとして利用し、wholesale mergeしない。
- bootstrapは一つの`PersonalityAgentId`と一つのagent VMを構成する。
- persistent volumeとstable private namespaceは`PersonalityAgentId`をowner keyに
  する。ephemeral process namespaceはさらに`ProcessGeneration`でfenceする。
- Compose project、volume、IPC、credentialを人格agentごとに分離し、同じ
  Workspaceかつ同じadministrative scopeの二agent fixtureでprivate stateと
  failure domainのisolationを証明する。
- `PersonalityAgentId`以外のconversation-scoped identity/config、既存agentでの
  予期しないhistory/memory欠落または復号失敗、placeholder approval、
  no-tool fallbackをfail-closedに拒否する。
  新しくprovisionされたagentの正規な空history/memoryは許可する。
- long-lived executor endpoint、fresh agent-scoped credential、central
  generation/Ready registryを実装する。
- parent agentの直接tool pathをproduction verticalで証明する。
- full subagent/PTY実装はT26 completionの自動条件にしない。ただし後続managerを
  差し込めないconcrete ownershipを凍結しない。

### T27 / PR #50

- quota、cgroup、execution registry、generation recoveryをagent VM単位にする。
- T26で証明済みのper-agent deployment boundaryを消費し、その境界を
  administrative-scope共有deploymentへ戻さない。
- administrative scope全体のaggregate quotaが必要な場合はcontrol planeが複数の
  per-agent deploymentを横断して強制する。

### T28 / PR #48

- API/browser replay transport自体は利用する。
- public route、session claim、event envelope、durable log keyを
  `PersonalityAgentId`へ移行する。
- browserで認証済みのhuman actorをcommand append時に捨てず、#78のprovenanceへ
  束縛する。
- UI chatが唯一のagent sessionへのdirect-address surfaceであることを
  Rust → Go → browserのrepresentative journeyで証明する。

### T29

- conversation reset、replacement conversation ID、conversation tombstone、
  reset DB transaction、conversation-wide crypto-eraseをportしない。
- conversation exportを`PersonalityAgentId`に束縛したcanonical life-log export
  へ移し、認可済みartifact archiveは現存payloadだけを含め、GC/tombstone済み
  handleを型付きmanifestとして表現する。
- closed tool-output artifact payloadのbounded high/low watermark GC、
  個別retention、明示的tombstoneを、人格identityとcanonical life-log
  lifetimeから独立して実装する。input attachmentへ同じGCを拡張しない。
- control plane外部tombstoneを正本とするsupervisor-owned agent deathへ置換する。
- history-preserving key rotationとagent deathを分ける。agent deathではDB、
  canonical life log、memory/provider-context鍵、artifact/private-workspace
  volume、credential、backupを完全purgeし、外部tombstoneを復元より先に
  再適用する。
- redacted export、検索・管理者access audit、KMS rotation/revocationの受入を
  維持する。

## Rejected alternatives

### Tenantごとの一つのmicroVM

同一Workspace内の複数人格agentのprivate state、failure domain、quota、
deletionを分離できないため採用しない。

### 全員が入るglobal shared POSIX VM

Sumi Workspaceの共有をfilesystem共有へ還元し、人格agentごとのprivate PCと
権限境界を失うため採用しない。

### 人格agentごとの複数conversation session

場所ごとに人格・時間・memoryを分断する。人格agentは一つのagent sessionに
継続し、各Workspace surfaceへ同じ個体が現れるため採用しない。

### Subagentを必須tool proxyにする

親agentの直接実行を破壊し、すべてのeffectを不要なchild lifetimeへ結合するため
採用しない。

### Subagentを別のPersonalityAgentにする

人格、public identity、Workspace membership、VM、life logを持たないbounded
workerという要件と矛盾するため採用しない。

### PTYを単発Bashのoptionだけで追加する

terminal identity、持続lifecycle、attach、stdin authority、recoveryのownerを
表現できないため採用しない。

### 人生ログだけを消して同じagentを残す

人生ログは人格を構成するため、identity continuityを偽ることになる。消去は
agent deathとして扱う。

## Non-goals

- 本ADRだけで共有Sumi Workspaceを実装すること。
- 今回のagent-foundation completionを全Issue完了へ拡張すること。
- T26 completionまでにsubagent、nested delegation、PTY、terminal UIをすべて
  実装すること。
- cross-personality-agent / cross-VM subagent delegation。
- terminal processを`ProcessGeneration` rollover後も生存させること。
- 選択的忘却、法的retention、agent cloning、inheritanceのproduct semanticsを
  このADRだけで確定すること。

## Review questions

このADRのreviewでは、実装詳細より次を確認する。

1. `PersonalityAgentId`、唯一のagent session、人生ログ、direct chatの関係。
2. Sumi Workspaceとagent-private VMの所有境界。
3. 人格agentを一人の主体として扱い、人間と同型にAI harnessを使わせる関係。
4. agent deathと、Workspace resource/viewの削除・key rotationの違い。
5. T26で今固定するseamと、#77/#81/#82へ段階導入する機能の境界。
