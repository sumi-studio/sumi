# ADR 0013: Tool invocation route・二種類のAutoReview・execution authority provenance

- Status: Accepted
- Date: 2026-08-10
- Amends:
  - [ADR 0008](0008-personality-agent-identity-and-execution-fabric.md)
  - [エージェント実装計画](../agent/implementation-plan.md)
  - [実装タスク](../../apps/agent/TASKS.md)
- Related:
  - [#54](https://github.com/sumi-studio/sumi/issues/54)
  - [#77](https://github.com/sumi-studio/sumi/issues/77)
  - [#134](https://github.com/sumi-studio/sumi/issues/134)

## Context

Sumiの人格agentはtool callごとに、通常実行するか、Humanへ承認を求める昇格実行に
するかを選べる。旧計画はこれをglobalな`ReviewerMode`と
`Allow / NeedsApproval / Forbidden`の一つのlatticeへ押し込み、次の異なる判断を
混ぜていた。

- agentが自分の権限で通常実行するexact callを安全reviewすること。
- agentがHumanへ承認要求を見せてよいかを事前reviewすること。
- Humanがagent自身のcapabilityによる実行へ同意すること。
- Humanのaccount/authorityをexact call一件だけagentへ委ねること。
- 将来のcallにも効くstanding Allow/Deny policyを作成・変更すること。

通常経路のreviewerが拒否または停止しただけでHumanへfallbackすると、agentが選ばなかった
昇格を基盤が勝手に起こす。一方、Elevatedを常にHuman-account delegationだと扱うと、既存の
「agent自身が行う操作へHumanが一回だけ同意する」Approvalを失う。invocation routeと
execution authority provenanceは直交する概念として扱う必要がある。

appのrole、membership、resource visibility、domain invariant、commit時認可はappが所有する。
agent foundationのtool policy、AutoReview、Human approvalはそれを代替せず、app APIを迂回する
authorityにもならない。二つは重なる安全境界である。

## Decision

### 1. validated ToolCallは一つのimmutable routeを持つ

provider出力からtool名・引数をstrict検証して実行可能な`ToolCall`を作る境界で、次の
いずれか一つを確定する。

```text
ToolInvocationRoute = Normal | Elevated
```

routeはpolicy評価、review、Human approval、実行開始、audit、recoveryを通じて不変である。
基盤はNormalを途中でElevatedへ変えず、ElevatedをNormalへ落として実行しない。agentは
各tool callごとにrouteを選ぶ。deploymentや人格agent全体へ設定するproduct-wide
`ReviewerMode`は置かない。

providerへ提示する全tool schemaは、app-owned input schemaを次のfoundation-owned envelopeで
包む。provider固有adapterはこの同じJSON shapeを各providerのtool-call argumentsへ載せ、
providerから戻った値を共通assemblerがstrictに検証してから`ToolCall`へ分離する。

```json
{
  "route": "normal | elevated",
  "input": { "...": "app-owned tool input" }
}
```

outer objectの必須fieldは`route`と`input`だけで、未知field、未知route、非objectの`input`、
route欠落を拒否する。欠落を暗黙にNormalと解釈せず、provider adapterごとの別語彙や
途中変換を作らない。requested authority provenanceはこのprovider-facing envelopeへ載せず、
route確定後の認証済みauthority経路で別に束縛する。

### 2. NormalはHumanへpromptしない経路である

Normalの決定論的policyは次の三値だけを返す。

```text
NormalPolicyDecision = Allow | Deny | Unmatched
```

- explicit `Allow`: exact callをagent自身のauthorityで実行する。
- explicit `Deny`: blockする。reviewerにもHumanにも送らない。
- `Unmatched`: **Execution AutoReview**へ送る。

Execution AutoReviewは「このexact callをagent自身のauthorityで通常実行してよいか」を
判定する。結果は`Allow | Block`であり、`Allow`だけがexact callを一回実行する。
timeout、transport error、schema不一致、判定不能を含むあらゆるnon-`Allow`はblockし、
Human approvalを一件も発行しない。

reviewerの`Allow`はauthority grantではない。agentが既に持つauthorityの範囲で、一回の
実行に対するsafeguardを通した証拠である。hard deny、sandbox、app APIの認可とdomain
invariantを広げない。

Normalのpolicy latticeに`Ask`、`NeedsApproval`、`Prompt`は置かない。Humanへ聞くかは
policy engineのfallbackではなく、agentがToolCallをElevatedとして提案したかで決まる。

### 3. Elevatedはagentが明示的にHumanへ承認を求める経路である

Elevatedはauthority sourceの名称ではない。agentが「このexact callはHumanの明示判断を
得てから進める」と選んだinvocation routeである。Humanへ表示する前に、Normalとは別の
prompt・schemaを持つ**Escalation AutoReview**を必ず通す。

genericな別`request_permission` toolは設けない。authority/consent requestとは、実際に
実行したいtarget ToolCall自身をElevatedとして提案することである。NormalのDeny/Blockから
基盤が自動でElevatedを生成・replayしてはならない。

このreviewが問うのは、操作を実行してよいかではなく、「この内容でHumanへ承認要求を
出してよいか。致命的な誤解、scope不整合、権限迂回がないか」である。

```text
EscalationReviewDecision = AskHuman | Block
```

- `AskHuman`: redacted projectionを持つ`ApprovalRequested`をdurableに発行する。
- `Block`: Humanへ何も表示せず、実行しない。
- timeout、transport error、schema不一致、判定不能を含むnon-`AskHuman`も`Block`である。

Escalation AutoReviewのpositive resultはexecution authorityではない。Gatewayが認証した
Human decisionがpending request、`tool_call_id`、route、canonical action digestと一致し、
未解決かつ未消費である場合だけ、current callを一回進められる。`ApproveOnce`はそのexact
callを一回進め、`DenyOnce`はblockする。重複、replay、stale decision、別actor、別action、
別scopeへapprovalを移したり、同じapprovalで二回実行したりしない。
Humanが対象、scope、引数を狭めた場合は元callを部分承認せず、digestの異なる新しい
canonical ToolCallとして再構築し、Escalation AutoReviewからやり直す。

### 4. routeとexecution authority provenanceを分ける

少なくとも次のprovenanceを区別する。

```text
ExecutionAuthorityProvenance =
  | AgentOwn
  | AgentOwnWithHumanConsent
  | HumanAccountOneShot
```

- `AgentOwn`: agent自身のmembership、role、credential、delegated capabilityの範囲で行う。
  Normalのexecutionはこれに限る。
- `AgentOwnWithHumanConsent`: capability/credentialの出所はagent自身のままだが、Elevatedで
  Humanがそのexact callへ一回同意した事実を伴う。既存Approvalの主要経路である。
- `HumanAccountOneShot`: 認証済みHumanのaccount/authorityを本当に使い、exact call一件を
  そのHumanの代わりに実行する。Sumi Workspaceの操作、外部service API、Browser Use、
  Computer Useのいずれでも同じ意味を持つ。

したがって`Elevated == HumanAccountOneShot`ではない。Elevatedのcurrent-call approvalを
解決するとき、要求されたcapability sourceとapp側の認可contractに従って後二者のどちらかを
durableに確定する。client payloadのactor名、agent credentialへのHuman名の付与、reviewer
allowをHuman-account authorityとして扱うことは禁止する。

`HumanAccountOneShot`は次をすべて満たす場合だけ成立する。

- GatewayがHuman actorを認証している。
- Humanのevent-time authorization contextが対象account/resource/actionを許可する。
- app/OS/browser側がそのHuman accountを正本として再認可する。
- grantがexact action digest、audience、scope、one-shot consumptionへ束縛される。

人格agentは一人の本人として一つのsingle threadを生き、canonical life logを持つのであって、
権限lifetimeに使えるdomain上のsession境界は存在しない。Human-account grantはexact callの一回の
消費またはpendingの取消・拒否・復旧時取消までであり、standing policyへ変換しない。

### 5. current-call decisionとstanding policy mutationを分ける

Elevatedのcurrent-call decisionは次だけである。

```text
CurrentCallDecision = ApproveOnce | DenyOnce
```

Humanが将来の操作に対するpolicyも管理できることはproduct requirementとして維持する。
current-call UIは「今回だけ承認」「今回だけ拒否」を扱い、standing policy管理は
「常に許可」「明示した期限まで許可」「永続拒否」と権限ruleの一覧・編集・削除を扱う。
standing policy mutationはcurrent-call decisionとは別の認証済みcommand/transactionにする。

Approval UIで両方を同時に選べることは許す。その場合もUIは、(a)current callをapprove/deny
するdecisionと、(b)将来へ効くstanding policy mutationを別のpayload・audit・commitとして
送る。旧`ApproveAlways { opaque rule }`のように一つの`ApprovalDecision`で両方を表さない。

standing policyのscope、rule語彙、precedence、最長duration、expiry/revocation semantics、
appごとの管理境界は未決である。「N分間」はsessionではなく認証済み絶対expiryへ変換する。
standing Allowは将来のagent-owned execution policyを変え得るが、Humanのaccountを将来callへ
貸し続けるgrantにはならない。

### 6. app authorizationとagent-foundation safeguardを二重に保つ

agent foundationはtool invocation route、決定論的policy、二種類のAutoReview、Human
current-call approval、execution authority provenance、sandbox、auditを所有する。app adapterは
actionをapp-owned APIへ変換し、appはHuman/agent membership、role、resource visibility、
domain invariant、commit時認可を正本として再検証する。

Normal/Elevated、Human approval、standing Allowのいずれでも次を迂回できない。

- managed hard denyまたはplatform safety boundary
- executor sandbox、network/filesystem/resource boundary
- app-owned commit-time authorizationとdomain invariant
- stale/revoked account、membership、role、resource binding

### 7. 二つのAutoReviewは型・prompt・cache・metricを共有しない

Execution AutoReviewとEscalation AutoReviewは、問いもpositive outcomeの効果も異なる。
次を別々の型として実装し、相互変換やfallbackを設けない。

- request typeとresult type
- system prompt、JSON schema、prompt version
- cache namespace/keyとinvalidation
- metric、false-positive/false-negative評価、audit label
- circuit breakerまたはavailability state（導入する場合）

Humanが書く固定prompt本文はRustの文字列literalへ埋め込まない。人格system prompt、Compact、
Execution AutoReview、Escalation AutoReviewを含むproductionのmodel-facing promptは、用途ごとの
専用`.md`を正本にし、Rustは`include_str!`、typedな動的evidence組立、version/digestの束縛だけを
持つ。ExecutionとEscalationの`.md`は共通base promptへ畳まず、個別にreview・version管理する。
JSON schemaやredacted action/transcriptなどの動的payloadはtyped構造として分離し、Markdownへ
文字列補間してprompt境界を曖昧にしない。

Executionの`Allow`をEscalationの`AskHuman`として再利用せず、Escalationのpositive resultで
実行しない。`StrictAutoReview`はproduct-wide execution modeにしない。Stage 1/Stage 2や
shadow二重判定を品質計測へ残すかは未決であり、残す場合もruntime authority semanticsを
変更しないinstrumentationに限る。

### 8. effectより前にrouteとauthority provenanceをdurableにする

各executionについて、少なくとも次をexternal effectより前、かつ
`ToolExecutionStart`と同じtransactionまでにdurableに固定する。

- immutable invocation route
- execution authority provenance
- canonical action digest
- policy version/hashとdecision
- reviewer kind/version、prompt/schema version、review result（reviewを実行した場合）
- Human decisionのcommand/request identityとevent-time authorization context（Elevatedのみ）
- Human-account grant identityとone-shot消費状態（`HumanAccountOneShot`のみ）

`prepared`は外部副作用前、`running`は副作用の有無が不明になり得るという既存の
durability/recovery境界を維持する。Elevated pendingはsoft steer、abort、crash recoveryで
従来どおり`Cancelled`へ閉じ、自動実行しない。current-call approvalまたはHuman-account grantを
消費して`running`へ進むtransactionとexecutor RPCの順序も、commit後にだけRPCを発火する
既存規則を維持する。

### 9. #134で使う語彙

permission/approval surfaceでは、少なくとも次を別名で扱う。

| 語彙 | 意味 |
|---|---|
| `ToolInvocationRoute` | `Normal | Elevated`。Humanへcurrent-call decisionを求めるかの経路 |
| `ExecutionReviewDecision` | Normal/Unmatchedの`Allow | Block`。Human promptを作らない |
| `EscalationReviewDecision` | Elevatedの`AskHuman | Block`。実行authorityを作らない |
| `CurrentCallDecision` | Gateway認証済みHumanの`ApproveOnce | DenyOnce` |
| `ExecutionAuthorityProvenance` | `AgentOwn | AgentOwnWithHumanConsent | HumanAccountOneShot` |
| `StandingPolicyMutation` | 将来のAllow/Deny policyを作成・更新・削除する別command。current-call decisionではない |

genericな`request_permission`、product-wide `ReviewerMode`、opaqueな`ApproveAlways`をこの語彙へ
加えない。実装時はwire/runtime/system promptを本ADRへ同じ変更単位で揃え、二重契約や
compatibility branchを作らない。

## Unresolved

次は本ADRで推測して埋めず、実装前に明示決定する。

1. agentがNormalとして提案したcallに対し、pre-routing / standing policyが
   「Elevatedとして新しいcallを提案し直すこと」を要求できるか。許す場合も、
   既存のNormal callを途中変換・replayしたり、Deny / BlockをHuman promptへ
   fallbackしたりせず、別のexplicit Elevated ToolCall proposalとして型付ける。
2. Normalのexplicit `Deny`を観測した後、同じactionをElevatedで提案できる条件。
3. reviewerのretry回数、timeout、circuit breakerの具体値。
4. `StrictAutoReview`という名称・機構をshadow instrumentationとして残すか。
5. policy bundleがmissing、stale、version mismatchのときのNormal/Elevated別挙動。
6. standing Allow/Deny policyのscope、語彙、precedence、expiry/revocation、管理UI、正本。

これらが未決でも、non-positive reviewをHumanへfallbackしないこと、routeとauthority sourceを
同一視しないこと、Human-account one-shotをstanding policyへ変換しないこと、hard deny・
sandbox・app authorizationを迂回しないことは確定事項である。

## Consequences

- agent自身の通常行為、Human同意付きのagent行為、Human accountを一回借りた行為をauditで
  区別できる。
- AutoReviewの停止や判定不能がHumanへの承認通知増加へ変換されない。
- Humanはcurrent callとstanding policy changeの違いを理解したまま判断できる。
- 既存のglobal `ReviewerMode`、`NeedsApproval → reviewer → manual fallback`、opaqueな
  `ApproveAlways` wire/UI/runtime経路は本決定に適合しないため置換が必要になる。
- appとagent foundationは権限語彙を奪い合わず、app commit時認可とfoundation safeguardを
  二重に維持する。

## Review questions

1. ToolCall自身がNormal/Elevatedをimmutableに持ち、基盤が途中でrouteを変更していないか。
2. Normal reviewerのnon-AllowがHuman promptを0件にしているか。
3. Elevated reviewerのpositive resultが実行ではなくHuman promptだけを作るか。
4. ElevatedとHuman-account authorityを同一視していないか。
5. Human-account delegationが認証済みHuman account、exact action、one-shot consumptionへ
   束縛されるか。
6. current-call decisionとstanding policy mutationが別のcontract/auditになっているか。
7. reviewer、Human decisionのどちらでもhard deny、sandbox、app authorizationを迂回しないか。
8. 二つのreviewerのprompt/schema/cache/metricが型で分離されているか。
9. route、authority provenance、version、Human event-time contextがeffect前にdurableか。
10. productionの固定prompt本文が用途ごとの`.md`を正本とし、Rustへinlineされていないか。
