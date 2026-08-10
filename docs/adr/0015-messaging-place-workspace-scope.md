# ADR 0015: Messaging placeのWorkspace scopeとDirect ChatのParticipant scope

- Status: Proposed
- Date: 2026-08-10
- Related:
  - [ADR 0008](0008-personality-agent-identity-and-execution-fabric.md)
  - [ADR 0009](0009-human-koseki-and-multi-user-auth.md)
  - [ADR 0011](0011-messaging-surface-and-agent-participation.md)
  - [#130](https://github.com/sumi-studio/sumi/issues/130)
  - [#132](https://github.com/sumi-studio/sumi/issues/132)
- Supersedes upon acceptance:
  - ADR 0011と[Messaging契約ドラフト](../messaging-contracts-draft.md)にある
    「DM / group DMはparentを持たないglobal place」という規定
- Preserves:
  - ADR 0009 §5のEmployer-only Direct Chat
  - ADR 0008の単一Workspace kindと`AppInstallationOwnerRef`

## Context

2026-08-01のFounderすり合わせでは、Direct ChatとMessaging DMを別Surface / placeとし、
Direct ChatをEmployer本人だけが使う人生ログの正面窓、Messaging DMをWorkspace外にも存在できる
global placeとした。

その後、AcceptedなADR 0008は、Messagingのinstallation ownerをWorkspace、Direct Chatの
installation ownerをParticipantと確定した。また現在のWorkspace verticalは、Workspaceの切替に
伴ってplaces、members、roles、settings、app installationsを同じscopeへ揃える。

parentlessなglobal Messaging DMには、entry surface、background activity、config、notification、
enable / disableを支配する一意なWorkspace installationが存在しない。複数の共有Workspaceで
installation stateが異なる場合、どのinstallationを正本とするかをserverが決定できず、clientの
current Workspaceから推測するとdomain identityがUI stateに依存する。

このADRはMessaging DMのscopeを変更する。過去のFounder intentを別の意味へ読み替えず、変更点を
明示する。Direct Chatのprivacy contractは変更せず、新しいWorkspace kindも導入しない。

## Decision

1. 現行productのMessaging catalog descriptorが許可するinstallation ownerは
   `Workspace(WorkspaceId)`のみとする。

2. `channel`、`dm`、`group_dm`を含むすべてのMessaging placeは、ちょうど一つの
   `WorkspaceId`を持つ。parentless / globalなMessaging placeは作らない。command、event、
   provenance、read marker、notification intentはこの`WorkspaceId`を明示する。

3. `dm`作成時には2名、`group_dm`作成時には全参加者が、そのWorkspaceのactive memberで
   あることをcommit時に検証する。active Workspace membershipは必要条件であり、DM本文の
   閲覧にはactive place membershipも必要とする。Workspace owner / adminであることだけでは
   DM本文を閲覧できない。

4. Workspace membershipがinactiveになった参加者には、そのWorkspace内のMessaging placeへの
   新規read、write、deliveryを許可しない。message、authorship、membership履歴は削除しない。
   Workspaceへの再加入だけで旧DMへのaccessを暗黙復元せず、明示的なplace admissionを必要とする。

   一対一DMでは、canonical pairのどちらかが、双方ともactive Workspace memberである状態で
   `ensure_dm`を明示実行したときだけ、欠けているcurrent place-membership tenureを同じtransactionで
   作る。Workspace再加入、一覧取得、DM画面を開くだけでは作らない。双方のWorkspace membershipと
   place membershipがactiveでない間は、そのDMへ新しいmessageをappendできない。pairが変わらないため、
   明示再admission後は同じDMの全履歴を再び閲覧できる。

   group DMへの再admissionは、activeなplace memberがactive Workspace memberを明示的に招待する
   app-owned operationとする。Workspace owner / adminであるだけでは実行できない。新しいtenureは
   admission時点の`visible_from_seq`を持ち、それ以前のgroup履歴を既定では読めない。過去履歴の共有は
   現参加者の同意と範囲を示す別operationが定義されるまで行わない。

5. 一対一DMの同一性は
   `(WorkspaceId, canonical unordered participant pair)`で定める。同じ2名がWorkspace Aと
   Workspace Bに所属する場合、AとBのDMは別placeであり、message、seq、read marker、
   notificationを共有・複製しない。

6. global Connectionは、identity discoveryやWorkspace invitationの根拠として将来利用できるが、
   それだけでMessaging DMの作成・参加権限を与えず、parentless placeの代替にも使わない。
   共有Workspaceがない参加者同士は、いずれか一つの通常のWorkspaceへ明示加入してから
   Messagingを使う。

7. Workspace-owned Messaging installationがdisabledまたはuninstalledの間は、そのWorkspaceの
   Messaging entry surface、agent tool、background deliveryを停止する。place identityとapp dataは
   保持し、再enable後は同じdataへ戻る。

8. Agentへ公開するapp toolは、各provider request境界でcurrent installation stateから作る
   immutableなtool snapshotに含まれる場合だけ提示する。in-flight requestは途中変更しない。
   disableと競合したcallはeffect直前のapp authorizationでfail closedにし、次のprovider requestから
   toolを除く。tool非表示はUX / capability advertisementであり、認可の代わりではない。

9. Direct Chatは`Participant(ParticipantRef)` ownedのEmployer-only Surfaceのままとする。他者を
   参加させず、Messaging DMと統合、転記、aliasしない。一人のHumanと明示加入した
   PersonalityAgentだけの利用形態は、通常のWorkspaceの縮退トポロジであり、`personal`
   Workspace kindではない。

10. Messagingの通知設定は本人が所有するが、`(WorkspaceId, ParticipantRef)`と任意のplace
    overrideでscopeする。Workspace A/Bの設定を共有またはcurrent Workspaceから推測しない。

11. Developer Workspace cutover前のcontract replacementとして実施し、global DMのbackfill、
    dual-read、compatibility branchは作らない。

## Workspace admission

Humanがcanonical UUIDを入力する画面や、公開participant directoryをcutover条件にはしない。
最初の明示admissionは、current tenureで`manage_members`を持つmemberが作るopaqueなsingle-use
invite linkとする。

- tokenは128 bit以上のentropyを持ち、serverはhashだけを保存して返却時に一度だけ平文を返す。
- expiryはserverが発行時に固定する絶対時刻で24時間、未使用tokenは発行者またはcurrent
  `manage_members` authorityを持つmemberがrevokeできる。
- linkの`GET`は最小限のWorkspace previewだけを返し、scannerやpreviewで消費しない。
- 認証済み参加者の明示`POST`が、自分自身の`ParticipantRef`をtransport認証から導出する。
- redemptionはtoken lock、未使用・未期限切れ・未失効、発行者の同じtenureがactiveかつ現在も
  `manage_members`を持つこと、base membership insert、token consumptionを一transactionで行う。
  inviteはroleを運ばず、role付与は別の`manage_roles` commandを必要とする。同じactorのretryだけは
  保存済みmembership tenureを返してidempotentにし、別actorへの再利用は拒否する。
- Secretary / Employment relationからmembershipを導出しない。Human UIとPersonalityAgent toolは
  同じredemption commandを使う。

短い再利用可能codeやparticipant直接検索は、brute-force対策、directory privacy、consentを別途
設計する必要があるため、このverticalへ混ぜない。

## Consequences

- 現行productにはcross-Workspace global Messaging DMが存在しなくなる。
- 将来global DMを復活させる場合は、Participant-owned Messaging installationまたは別catalog appと、
  そのlifecycle・notification・privacyを別ADRで定義する。Workspace installationやclient current
  contextから暗黙導出しない。
- Direct Chatの私信性と人生ログ境界は維持される。
- `personal | organization`のWorkspace kindやkind変換は不要である。

## Rejected alternatives

- global DMにclientのcurrent Workspaceを後付けする: domain identityとauthorizationがUI stateに
  依存するため却下。
- personal Workspace kindを追加する: ADR 0008の単一Workspace modelに反するため却下。
- Direct ChatをSecretary DMへ統合する: privacy、life-log、provenance境界を壊すため却下。
- 現incrementでMessagingをWorkspace / Participant両scopeへinstallする: global DM用の別lifecycleを
  新設する別product decisionであり、current verticalのcompletion条件を超えるため却下。
