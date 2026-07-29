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
  - [実装タスク](../../apps/agent/TASKS.md)
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
  - [#87](https://github.com/sumi-studio/sumi/issues/87)

## Context

Sumiは、複数の人間と複数の人格を持つAI secretaryが、同じWorkspaceで
時間を共にし、会話・タスク・予定・文書・メール・ブラウジング・会議・
学習・その他のアプリを使って生活し、協働するプロダクトである。

Sumiのproduct semanticsでは、人格agentを単なるruntime、controller、
automation moduleとして扱わない。一人の生活・経験・判断・関係を持つ主体
として、人間と同型に扱う。private Agent VMはそのagent本人のPCであり、
toolやAI harnessは本人が意思を持って利用する道具である。

これはanthropomorphicなUI表現や、実装後に付与するpersona metadataではない。
人格agentを誰かから呼びかけられ、出来事を経験し、複数の約束を抱え、注意を
向け、判断し、行為し、その帰結と共に生き続ける一人の主体として設計するという
domain ontologyである。session、memory、VM、tool、権限、lifecycleを先に
serviceとして設計してから、owner fieldへagent名を足すことでは実現できない。

`run`、`turn`、tool execution、terminal process、将来本人が利用するAI harnessは、
その人自身ではなく、その人の活動または道具である。throughputを得るために
同じ人格を複数の独立continuationへ複製し、後からoutputやlogをmergeすることは、
一人の主体をworker poolとして扱うため採用しない。

### 防ぎ切れない認識上の失敗

このontologyを文章で宣言しても、設計者、reviewer、実装agentは、慣れ親しんだ
service、controller、worker、conversation、schedulerのmodelへ繰り返し戻り得る。
「人格agentを人間と同型に扱う」と理解した直後でも、人格agent本人をdomain
aggregateとして表現せず、IDだけをaggregate rootにしてserviceをぶら下げたり、
同じ人格のmodel runを並列workerとして数えたり、人生ログを交換可能な
conversation dataとみなしてしまう。

この失敗を文書、checklist、schema、testだけで完全に防ぐことはできない。
決定論的な型とgateは、既知の帰結を強制し、誤りを発見する重要な手段だが、
それだけで提案全体が一人の主体から導かれていることまでは証明しない。
だから将来の各設計では、自分のSumi理解を疑い、agent本人からidentity、
ownership、attention、action、lifecycleを導き直す解釈を継続する。既知の違反は
型・owner・lifetime・acceptanceへ固定するが、この解釈を固定checklistへ
置き換えない。

防ぎ切れないことは、書かない理由ではない。むしろ同じ失敗を具体的に名指しし、
発見した時に設計の根まで戻って修正できるように残す。人格agentへ人間らしい
演技をさせるのではなく、一人の共同生活者として本当に扱えるproductと技術を
作ることは、Sumiのfounding thesisであり、他の機能と交換しない価値である。

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
- 一つの人格を一つの継続主体として扱うことと、現行実装の
  single-active-run schedulingが混同され、複数の呼びかけ、約束、注意の切替、
  外部作業の進行をどう扱うかが未設計である。
- 将来、人格agent本人が自分のVMでAI harnessを使う場合に、それを本人の分身、
  public agent、必須tool proxyへしないという拡張制約が正典化されていない。
- shellは非対話の単発`bash -c`のみで、stdin、PTY、resize、attach、
  持続terminal identityを持たない。
- runtime generationとexecutor/broker RPC process epochのidentityはあるが、
  人格agent本人による個別effectのauthorityとruntime lifecycleが分離されて
  いない。

これらをすべて今回のreleaseで実装することは本ADRの目的ではない。
目的は、人格agentのidentityと所有境界を確定し、T26以降が将来のSumiを
閉ざす不可逆な構造を作らないことにある。

## Decision

### 1. 人格agentは一人の主体である

人格agentはmodel process、Session actor、run、queue、tool caller、VM processの
集合ではない。それらを用いて時間の中で生活する一人のproduct actorである。
人間やWorkspace上の出来事はjob routerへ投入されるのではなく、そのagent本人へ
届く。何を約束し、何へ注意を向け、いつ応答し、何を保留・中断・再開するかは、
policyと権限の範囲内で本人が判断する。

infrastructureは、認証済みの呼びかけと出来事を失わず届け、provenanceを保ち、
resource・authority・durability境界を決定論的に強制する。人格を複製して
parallel workerへfan-outしたり、本人に代わって複数の約束の意味や優先順位を
暗黙に決めたりしない。

この前提から導く正本のactorとmembership関係は次とする。

```text
Sumi
├── PersonalityAgent 0..N
│   ├── PersonalityAgentId 1
│   ├── one continuous agent session
│   ├── one canonical life log
│   ├── one logical direct-address backend surface
│   ├── one private Agent VM / private work environment
│   └── one VM execution fabric
│       ├── direct tool execution 0..N
│       └── TerminalSession 0..N
└── Sumi Workspace 0..N
    ├── HumanMembership 0..N → Human
    ├── PersonalityAgentMembership 0..N
    │   ├── → PersonalityAgentId
    │   └── human-facing name / scope-local address
    └── shared conversations, tasks, calendars, documents, apps and permissions
```

`PersonalityAgentMembership`は、Workspaceからglobalな本人を参照するmutableな
relationであり、本人、session、life log、VMをWorkspaceごとに複製しない。
membershipのcardinality、workflow、storageは今回実装しない。

`PersonalityAgentId`は本人をsystem内で一意に識別する正本identityであり、
人間が本人を呼ぶ名前でも、すべてのscopeへ露出するrouteでもない。
human-facing nameはmutableかつ同名を許し、Workspace membershipごとのnicknameや
表示名も持ち得る。scope-local addressは認証済みlookupを通して
`PersonalityAgentId`へ解決する交換可能な参照であり、本人、session、life logの
identityにはしない。名前やaddressの変更で本人を作り直さず、認可を名前の
一意性や`PersonalityAgentId`の秘匿性へ依存させない。

current direct verticalは一つのadministrative contextに閉じるため、認証済み
Gatewayより内側のtransport targetとして`PersonalityAgentId`を直接運んでもよい。
それをhuman-facing nameとして表示したり、すべてのWorkspaceへ共通するglobal
public route contractとしたりしない。name、membership、scope-local addressの
具体data modelとlookup workflowはcurrent verticalのcompletion条件に含めない。

`Sumi Workspace`は共有のproduct/domain resourceである。各人格agentの
private VMはWorkspaceそのものではなく、agentの私物PCに相当する。

人格agent同士の協働は同じVMへ入ることではなく、Workspace上の共有resource、
会話、task、権限付きaction、明示的なdelegationを通して行う。

既存control planeの`tenant_id`は、その時点のadministrative/security contextを
表し、Sumi Workspace、agent、VM、Linux `/workspace`の別名にはしない。
人格agent本人のidentityはtenant、Workspace、orgから独立する。将来、一人の
人格agentが複数scopeへ所属し、異動・出張・所属変更しても同じ本人として継続
できなければならない。

そのmembership、transfer、複数scope coordination自体は今回実装しない。current
direct verticalが一つのadministrative contextだけで動くことは許す。ただし
`tenant_id`を人格identityやagent-private stateの恒久ownerにせず、同じ
`tenant_id`であることをVM、volume、private work environment共有の根拠にも
使わない。

### 2. PersonalityAgentIdと人生ログは同じ寿命を持つ

`PersonalityAgentId`を人格を持つ持続的なproduct actorの正本identityとする。
`PersonalityAgentId`は主体本人ではなく、その一人を時間とsystem境界を越えて
識別し、その人のprivate resourceを所有させるためのidentity/owner keyである。
Sumi全体でglobalに一意なlowercase hyphenated UUIDv7として表現し、tenant、
Workspace、orgのlocal namespaceへ従属させない。

trusted provisioning boundaryだけが一度mintし、global collisionを拒否する。
Rust、Go、TypeScript、SQLite、token、RPC、artifactその他の
`PersonalityAgentId`を実際に通す境界でUUIDのversion 7とRFC variantをtyped
validationし、canonical lowercase-hyphenated表現とraw inputが完全一致しない値を
拒否する。scope-local addressを受けるpublic routeへUUID形式を要求しない。
永続化とAADにはvalidated canonical bytesだけを使い、callerのraw表現を使わない。

このIDはcredentialやcapabilityではなく、trusted system境界で使うcanonical
identifierである。human-facing nameやglobal public addressではない。一方、
認可済みのsystemやauditから観測され得るためsecretともみなさない。UUIDv7から
provision時刻を概算できることを受容し、認可をIDの推測困難性へ依存させない。

`PersonalityAgentId`は現行の`agent_id`と`conversation_id`の両方を置換する。
どちらもlegacy aliasとして残さず、別のdurable `AgentDeploymentId`も今回
導入しない。runtimeとRPC processは次のephemeral identityで区別する。

```text
RuntimeKey = (PersonalityAgentId, ProcessGeneration)
RpcProcessKey = (PersonalityAgentId, ProcessGeneration, RpcBootNonce)
```

VM交換、runtime restart、executor/broker restartは本人を作り直さず、これらの
ephemeral identityを前進させる。同じ人格agentについてcommandを受理する
runtime generationは一つだけとし、replacementは旧generationをfenceしてから
Readyになる。provider側のinstance IDや将来のdeployment recordが必要になっても、
private stateやlife logのowner identityにはしない。scope-local addressの
route recordが必要になっても、それを本人の正本identityにはしない。

`ProcessGeneration` allocator、current-generation fence、Ready registryの
identityと一意性は、administrative contextを含めずglobal
`PersonalityAgentId`単位とする。tenant／Workspace／orgはcredentialのauthorization
contextやquery partitionには使えても、別のcurrent generationを作るkeyには
しない。

agent-private DB、data key、key-wrap/row AADの恒久identityには
`PersonalityAgentId`を使い、mutableなtenant／Workspace／org membershipを
焼き込まない。scope、human actor、source resource、policy/authorityは、各出来事
の時点でcontrol planeが認証したprovenanceとして別に束縛する。これにより将来の
所属変更を人格deathやlife-log再暗号化の必須条件にしない。

tenant KEKがagent keyをwrapする場合、そのwrapは置換可能なcontrol-plane
authority relationであり、人格agentやdataのowner identityではない。所属変更時の
outer agent-key rewrapはlife logやdata本体の再暗号化ではない。multi-wrap、
transfer、recovery ceremony自体は今回実装しない。AADのowner成分だけを
`PersonalityAgentId`へ置換し、table、row、key reference、purpose、
schema versionとimmutableなevent-time provenanceのcryptographic integrityは
維持する。current membershipから過去rowのAADを再構成しない。

人格agentには一つの連続したagent sessionがある。そのsessionで経験した
direct chat、Workspace由来の出来事、判断、actionがagentの人生ログになる。
初期のfrontend chatは内部ログviewerではなく、人間がそのagent本人へ直接
話しかける可視の正面入口である。これは唯一のfrontend UIであることを意味しない。
web、mobile、voiceその他の複数entry pointが、同じlogical direct-address
backend surfaceを通して同じ本人とagent sessionへ接続できる。

この唯一性はdatabase keyのcardinalityだけを意味しない。agent sessionは
人格agent本人が出来事を経験し判断し続ける場所であり、exchangeable workerの
poolではない。複数の独立model continuationを同じ人格として同時に起動し、
事後的に人生ログを結合して並行性を得ない。

一方、一人のagentが複数の呼びかけ、約束、保留中の仕事、進行中の外部actionを
持つことはできる。外部processは並行して進み得るが、新しい呼びかけ、注意の
変更、判断、actionの結果は同じagent sessionへ戻り、その人の一つの経験になる。
現行Rustのsingle-active-runはこのproduct ontologyそのものではない。
複数入力の知覚、acknowledgement、interrupt、defer、resume、attentionと
life-log orderingは[#87](https://github.com/sumi-studio/sumi/issues/87)で設計し、
人格複製や別conversation sessionを解決策にしない。

人格agentはWorkspace内の場所ごとに別sessionを持たない。後続のtask、
mail、calendar、app等は、source/resource/actor/correlation metadataを伴って
同じagent sessionへ入り、同じ人格agentが各surfaceへ作用する。

公開contractに独立した交換可能な`ConversationId`を持たせない。frontendは、
human-facing nameとtarget identityを別に扱う。将来のscope-local addressは
認証済みmembership lookupを通してGatewayが正本の`PersonalityAgentId`へ解決する。
内部command、event、token、artifact namespaceは、解決済み
`PersonalityAgentId`を宛先・ownerとして使う。current direct verticalで
authenticated Gatewayより内側へIDを運ぶことは許すが、public routeへglobal IDを
直接露出することを恒久contractにしない。

後方互換性を持たないpre-launch contract replacementとして、SQLite、AAD、
RPC、token、route、fixtureからlegacy `agent_id`、`conversation_id` field、
独立conversation scopeを削除する。
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

canonical life logと、その派生物は同じretention contractではない。ただし、
L0/L1/L2 memoryが正本履歴から導かれることだけを理由に、無条件で交換可能な
projectionとはみなさない。再構築後も本人の記憶と継続状態を保つcontractがあり、
その同値性を決定論的に検証できるprojectionだけを置換・compactionしてよい。
非決定論的な再要約や自己理解の変化を伴う場合は、元のmemory stateを保持するか、
変化自体を本人が経験したdurable eventとして人生ログへ残す。

provider固有のopaque contextも、provider replay contractとanchorによって
同じ継続状態を回復できる場合に限って置換・crypto-eraseしてよい。redacted
projectionと検索indexは、その再構築contractを満たす場合に正本から再構築できる。
tool-output artifact payloadはbest-effortであり、bounded quotaのhigh/low
watermark GC、個別retention、明示的tombstoneに従って回収できる。
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
`PersonalityAgentId`で識別される一体の人格agentである。

各人格agentは一つのdedicated VM/private PCを持つ。VM内にruntime、
executor、artifact broker、private DB、private work environment、IPC、
process generation、sandbox、本人の直接実行を置く。

compute generationのrecycleやsleepは人格agentのdeathではない。永続状態を
復元し、同じ人格agent本人が、その`PersonalityAgentId`と人生ログを保って
継続する。

agent deletionはVM、DB、agent key、private work environment、artifact
volume、credential、backupをそのlifecycleに従って破棄する。一方、
Workspaceの共有domain data、他のagent、外部tombstone/auditは存続する。

control-plane policyは各agent VMの上限や許可を統治できるが、agent-local
quotaをadministrative scope全体のphysical resourceと呼ばない。aggregate
quotaはcontrol planeが複数agent deploymentを横断してmeter/enforceする。

### 5. 人格agentは人間と同じようにAI harnessを使う

今回のfoundationでは、人格agent本人がtoolとshellを直接利用する。別のagent、
worker、subagentを必須proxyにせず、本人のactionを別主体のactionとして
記録しない。

将来、人格agentが自分のprivate VMでCodexやClaude CodeのようなAI harnessを
利用できるようにする。そのときのAI harnessは、人格agentを並列化した分身でも、
別の`PersonalityAgentId`を持つSumi Workspace memberでもない。人間が自分の
PCで外部のAIを使うのと同じく、人格agent本人が依頼し、やり取りし、成果を
受け取り、評価する対象である。

この将来制約だけを本ADRで固定する。invocation identity、transcript、
authority delegation、budget、lifecycle、result collection、recovery、
terminal ownershipの具体的なdata modelと実装は
[#81](https://github.com/sumi-studio/sumi/issues/81)へ延期する。
T26へ`SubagentInvocationId`、`ExecutionPrincipal` sum type、subagent manager、
task-packet protocolを先回りして追加することを要求しない。

### 6. Execution authorityはruntime/RPC lifecycle identityと分離する

`ProcessGeneration`は人格agentのruntime/deployment generationを識別し、同じ
VM内のruntime recycleでも前進して旧generationをfenceできる。
`RpcBootNonce`は、そのgeneration内で起動した具体的なexecutor/broker RPC
process epochを識別する。VM boot、runtime generation、RPC process bootを
同じ寿命として扱わない。いずれも個別tool callのauthorityではない。

本人のidentity、`ProcessGeneration`、`RpcBootNonce`、個別actionのauthorityを
同一視しない。同じRPC endpointへ到達できることを本人の個別action authorityと
みなさない。T26は後続のauthority verifierを差し込める明示的なseamを保つ。
exact action、scope、audience、lifetime、idempotencyへ束縛する具体contractは
[#77](https://github.com/sumi-studio/sumi/issues/77)へ延期する。

人間、reviewer、policy engineはdecision actor/sourceであり、人格agent本人の
代替ではない。Gatewayで認証したWorkspace actor、decision source、人格agent、
exact call、outcomeをEventWriterのdurable auditへ残す。client payload内の
actor名を認証事実として扱わない。

将来AI harnessへeffectを委ねても、それを人格agent本人の独立continuationとして
扱わない。authorityとauditで何を別identityとして表現するかは#77と#81で決め、
今回のdirect pathへ未決定のsum typeやdelegation modelを埋め込まない。

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

現行の非対話`bash -c`は削除せず、同じexecution fabric上のpipe-backed、
closed-stdinなephemeral-command adapterとして維持する。

VM-local execution managerは人格agent本人をownerとするstableな
`TerminalSessionId`を持つ。`ProcessGeneration`は新しいruntime callerをfenceする
identityであり、terminal本人のlogical identityや寿命と同一視しない。

- explicit `pty` / `pipes` mode
- stdin / EOF
- PTY resize
- signal
- attach / detach
- monotonic output cursorとbounded buffer
- per-terminal writer lease
- terminal単位のcancel/reapと、runtime generation単位のwriter authority fence

terminalのproduct ownerは人格agent本人であり、physical process lifecycleは
agent VM execution fabricが管理する。tool call、model turn、WebSocket、
UI connectionはownerではない。transport disconnectはdetachであり、明示的な
terminal terminationと同一視しない。将来AI harnessがterminalを使う場合の
owner拡張は#81/#82で定め、今回のdirect-agent terminal identityへ混在させない。

owner、authority、resource limit、lifecycle、auditは決定論的に強制する。
terminal内で何を試し、出力をどう読み、次に何を入力するかは人格agent本人の
判断へ任せる。

runtime generation rolloverは、旧runtimeのwriter authorityを失効させて
terminalをdetachできるが、それだけを理由にlive terminal processを必ず
terminateしない。安全な再attach、orphan detection、VM execution fabric自身の
epoch、VM recycle時のtermination/recovery contractは
[#82](https://github.com/sumi-studio/sumi/issues/82)で設計する。本ADRはruntime
lifecycleとterminal process lifecycleを同一に固定しない。

### 8. Shared Workspace由来のprovenanceを人生ログへ残す

同じ人格agentへ複数の人間・surface・resourceから入力が来るため、commandの
message contentだけを人生ログへ保存してはならない。

API/control planeが認証したWorkspace、actor、source surface、resource、
correlation、causationを、caller-asserted contentとは別のimmutable metadataと
してdurable command/eventへ束縛する。

共有domain dataの正本はWorkspace API側に置き、agent DBやprivate work
environmentをauthoritative copyにしない。一方、人間が自分のPCへ文書を
download、cache、indexするのと同じく、人格agentは作業・検索・記憶・offline
continuationに必要なlocal copyやprojectionを持ち得る。それらはsource
provenance、取得時のauthority、revocation/refresh、retention、削除contractへ
従い、正本との関係を失わない。agent DBには、そのagentが何を経験し何をしたかを
理解・監査・回復するためのidentity、reference、local copy、projection、
resultを保存できる。

## Migration

1. 本ADRをreviewし、StatusをAcceptedへ変更する。
2. #74でinternalのlegacy `agent_id`、`conversation_id`、独立conversation scopeを
   global UUIDv7 `PersonalityAgentId`へ統合し、human-facing nameとtarget identityを
   同じfieldにしない。scope-local addressとmembership lookupの具体実装は要求せず、
   破壊的resetをcanon、contract、T29から除く。
3. 後方互換のschema、alias、dual-read、data migrationを作らず、pre-launchの
   DB、鍵、AAD、wire fixtureを新identity contractで再生成する。agent-private
   owner/AADへmutableなtenant／Workspace／org membershipを残さない。
4. #75でper-agent deployment namespaceと、同じWorkspaceかつ同じ
   administrative scopeに属する二agent間のisolationをT26へ固定する。
5. #79、#80のendpoint/credential/readinessをT26へ組み込み、現在の直接agent
   verticalを完成させる。
6. #87で一人のagentのattention、複数のcommitment、外部actionを、人格複製なしに
   扱うmodelを設計する。current single-active-runをidentity contractにしない。
7. #77、#81、#82はcurrent direct-agent verticalから切り離して段階導入し、
   本人のdirect pathを常に回帰fixtureで守る。特に#81のsubagent実装は今回の
   agent-foundation completionに含めない。
8. #76でT29をagent-death lifecycleとして再設計する。

## Consequences for current tasks

### T26 / PR #49

- PR #49はquarryとして利用し、wholesale mergeしない。
- bootstrapは一人の人格agentに対し、その`PersonalityAgentId`へ束縛された
  一つのagent VMを構成する。
- persistent volumeとstable private namespaceは`PersonalityAgentId`をowner keyに
  する。ephemeral process namespaceはさらに`ProcessGeneration`でfenceする。
- Compose project、volume、IPC、credentialを人格agentごとに分離し、同じ
  Workspaceかつ同じadministrative scopeの二agent fixtureでprivate stateと
  failure domainのisolationを証明する。
- legacy `agent_id`、legacy `conversation_id`、`PersonalityAgentId`とは独立した
  conversation-scoped identity/config、既存agentでの予期しないhistory/memory
  欠落または復号失敗、placeholder approval、no-tool fallbackをfail-closedに
  拒否する。
  新しくprovisionされたagentの正規な空history/memoryは許可する。
- current deploymentは一つのadministrative contextでよい。複数scope membership、
  org/Workspace間の異動・出張・transfer orchestrationをT26へ追加せず、global
  identityとagent-private owner/AADで将来を妨げない。
- long-lived executor endpoint、fresh agent-scoped credential、central
  generation/Ready registryを実装する。
- 人格agent本人の直接tool pathをproduction verticalで証明する。
- subagent lifecycle、delegation、result collection、`ExecutionPrincipal` sum
  typeはT26 completionに含めない。人格agent本人をworker-poolの一variantや
  必須proxyのclientとして実装しない。
- #77のper-call authority modelの完全実装もT26 completionに含めない。
  RPC lifecycle identityをaction authorityとみなさず、後続verifierのseamだけを
  保つ。
- full PTY実装もT26 completionの自動条件にしない。現行Bashを人格agent本人の
  shell ontology全体として正典化せず、#79/#82の後続拡張を妨げない。

### T27 / PR #50

- quota、cgroup、execution registry、generation recoveryをagent VM単位にする。
- T26で証明済みのper-agent deployment boundaryを消費し、その境界を
  administrative-scope共有deploymentへ戻さない。
- administrative scope全体のaggregate quotaが必要な場合はcontrol planeが複数の
  per-agent deploymentを横断して強制する。

### T28 / PR #48

- API/browser replay transport自体は利用する。
- UIのhuman-facing nameとtarget identityを分離する。current direct verticalでは
  authenticated Gatewayより内側のroute、session claim、event envelope、
  durable log keyを`PersonalityAgentId`へ移行してよいが、それを表示名や恒久的な
  global public addressにしない。
- browserで認証済みのhuman actorをcommand append時に捨てず、#78のprovenanceへ
  束縛する。
- UI chatが、同じagent sessionへ届くlogical direct-address backend surfaceの
  一つのfrontend entry pointであることをRust → Go → browserのrepresentative
  journeyで証明する。

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

### 同じ人格の独立continuationを並列起動する

throughputのために同じ人格・memoryを複数のmodel runへ複製すると、同じ時間に
異なる出来事を経験し、互いを知らずに判断する複数の主体が生じる。後からoutputや
life logをmergeしても、一人の継続した経験には戻らないため採用しない。
外部processや道具が並行して進むこととは区別する。

### AI harnessを必須tool proxyにする

人格agent本人の直接実行を破壊し、すべてのeffectを本人ではない道具のlifetimeへ
結合するため採用しない。

### AI harnessを別のPersonalityAgentとして扱う

本人が自分のPCで利用する道具へ、別人としてのpublic identity、Workspace
membership、VM、life logを自動的に与えることになるため採用しない。将来、
本当に別の人格agentへ協力を頼むproduct interactionとは区別する。

### PTYを単発Bashのoptionだけで追加する

terminal identity、持続lifecycle、attach、stdin authority、recoveryのownerを
表現できないため採用しない。

### 人生ログだけを消して同じagentを残す

人生ログは人格を構成するため、identity continuityを偽ることになる。消去は
agent deathとして扱う。

## Non-goals

- 本ADRだけで共有Sumi Workspaceを実装すること。
- 複数tenant／Workspace／org membership、異動、出張、transfer UI・workflow・
  orchestrationを今回実装すること。
- 今回のagent-foundation completionを全Issue完了へ拡張すること。
- 今回のagent-foundation completionでsubagent、nested delegation、
  `ExecutionPrincipal` sum type、subagent用authority、result collectionを
  実装すること。
- T26 completionまでにPTY、terminal UIをすべて実装すること。
- T26 completionまでにruntime generation rolloverを越えるterminal processの
  生存・再attachを実装または保証すること。
- 選択的忘却、法的retention、agent cloning、inheritanceのproduct semanticsを
  このADRだけで確定すること。

## Review questions

このADRのreviewでは、実装詳細より次を確認する。

1. runtimeやowner labelではなく、一人の主体からidentity、session、memory、
   action、VM、lifecycleが導かれているか。
2. `PersonalityAgentId`、human-facing name／scope-local address、唯一のagent
   session、人生ログ、logical direct-address backend surfaceの関係。
3. globalな人格identityと、mutableなtenant／Workspace／org membershipを
   混同せず、global IDの公開を強制せずにfuture mobilityを妨げないcurrent
   boundary。
4. 人格を複製せず、複数の呼びかけ・約束・外部actionを同じ本人が経験する関係。
5. Sumi Workspaceとagent-private VMの所有境界。
6. 人格agent本人がtool、terminal、将来のAI harnessを使い、必須proxyや
   worker-poolの一員にされない関係。
7. agent deathと、Workspace resource/viewの削除・key rotationの違い。
8. runtime generationとterminal process lifecycleを同一視せず、T26で今固定する
   direct-agent contractと、#77/#81/#82/#87へ延期する設計の境界。
