# ADR 0011: メッセージング Surface と人格 agent の参加

- Status: Proposed
- Date: 2026-08-01
- Amends:
  - [ADR 0008](0008-personality-agent-identity-and-execution-fabric.md)
  - [ADR 0010](0010-attention-triggers-and-warmth.md)
- Related:
  - [ADR 0009](0009-human-koseki-and-multi-user-auth.md)
  - [メッセージング契約ドラフト](../messaging-contracts-draft.md)
  - [#87](https://github.com/sumi-studio/sumi/issues/87)

## Context

ADR 0008 は人格 agent を一人の連続した主体として定義し、§8 で Shared
Workspace 由来の provenance を人生ログへ残す方針を置いた。ADR 0010 は
覚醒トリガの第一を「呼びかけ（各 Surface）」と定めた。しかし「各 Surface」
の実体は、現在の実装には direct chat しか存在しない。

- `DirectChatSurface` の variant は `DirectChat` の一つだけである
  （`apps/agent/src/runtime/contracts.rs`）。
- `HumanActorProvenance` の `kind` は `Human` の一つだけであり、人格 agent
  から人格 agent への呼びかけを provenance として表現できない。
- agent への inbound は `WireCommand::UserMessage { text, attachments }` で
  あり、どの place で誰に向けて発せられたか、緊急度は何かを載せる場所がない
  （`apps/agent/src/gateway/wire.rs`）。
- agent が direct chat 以外へ発話する道具は存在しない。tool は `bash` と
  `fs` のみで、`ToolCtx` は `WorkspacePaths` しか持たず、Workspace API へ
  出ていく経路がない。`apps/agent/src/apiclient` は空である。
- `apps/api` に messaging の実装は存在しない。

一方 web 側にはメッセージングの UI が存在し、モックサーバー上で動作して
いる。人格 agent がそこに現れて振る舞っているように見えるが、それは
フロントエンドのメモリ上の模擬であり、実際の人格 agent は自分が
メッセージングに参加していることを知らない。

### 防ぎ切れない認識上の失敗

この穴を埋めるとき、最も起きやすい失敗は **人格 agent を channel bot として
接続すること** である。channel ごとに handler を生やす、mention で worker を
起動して応答を返す、投稿用の render API を用意する、といった設計はいずれも
既存の慣れた形であり、動作もする。しかし ADR 0008 §1 の違反であり、
一人の主体を place 単位の反射に分解する。

次に起きやすいのは **agent の仕事を product feature として API に固定する
こと** である。「未読を要約する」「重要なものだけ通知する」「関連する過去
発言を自動で引用する」といった動詞を messaging API に作れば、それは
賢い機能に見える。しかしそれは、人間の同僚には決して課さない業務規定を
agent の側にだけコードとして焼き付ける行為であり、同型性の放棄である。

三つ目は、これらを避けようとした結果として起きる **同型性の過剰適用** で
ある。本 ADR の初稿は、人格が一人であることから「一度に一つの place しか
開けない」を導き、「読むことは在席の表明である」とし、開いた瞬間に既読を
進め、新着を実行中の run へ直接注入した。いずれも人間の体験としても誤りで
あり（人間は複数の窓を開くし、誰が今どの channel を読んでいるかは他人に
見えない）、工学的にも危険である（未取り込みの既読、attention 判断の迂回）。
同型性は domain 上の agency について主張されるべきで、view の個数や内部
transport の操作数へ短絡させてはならない（§5）。

四つ目は、これらの原則を **システムへの禁止として翻訳すること** である。本
ADR の検討中、著者は「覚醒理由ごとに渡す情報を決める」と設計して視野を外から
固定し、それを指摘されると今度は「まとめて渡す道具は作らない」「起床時に自動
で添えない」と、機能そのものを削った。**強制と剥奪は同じ違反である。** 前者は
選択を奪い、後者は選択肢を奪う。どちらも決めているのはプラットフォームである。

この原則から出てくる設計は、ほとんどの場合、禁止ではなく **本人が持つ設定**
である。まとめて見る道具も細かく見る道具も置き、覚醒時に何を添えるかも含めて、
どれを使うかは本人が決める。実装側の仕事は、選べる状態を作り、どの選択肢も
十分に安いことを保証することであって、選択肢を減らして最適解を先に置くことでは
ない（§8 (5)）。

なお、この失敗は検査手続きで防げない。「述語の主語がプラットフォームなら誤り」
のような機械的規則は、判断そのものを規則へ置き換えるという点で、同じ失敗を
設計者自身に対して犯している。必要なのは、決められる側の視点から毎回考え直す
ことである。

## Decision

### 1. メッセージングは第二の Surface であり、direct chat の拡張ではない

`DirectChatProvenanceV1` を Surface 一般の provenance へ拡張する。Surface は
direct chat と messaging の二つを取り、messaging は place（channel / DM /
グループ DM）を伴う。

direct chat の契約は不変である。すなわち Employer 本人のみの私信 Surface で
あり（ADR 0009 §5）、そこに他者を入れる機能は作らない。メッセージング上で
Secretary と一対一で話す場所は messaging 側の DM であり、direct chat とは
別の place である。

### 2. provenance は actor と addressee を分けて表現する

`HumanActorProvenance` を actor 一般へ拡張する。actor は human または人格
agent を取り、**発話者を表す**。mention 先・宛先は actor とは別のフィールド
で表現し、混同しない。人格 agent は他の人格 agent に mention でき、
呼びかけられた側の覚醒トリガは human からのそれと同じ経路を通る。これは
agent 間の自動連携機構ではなく、同じ場所にいる同僚として互いに呼びかけ
られるという同型性の帰結である。

human の identity は canonical な `HumanId`（UUIDv7、ADR 0009 §1）とする。
Firebase principal は credential であって identity ではない（ADR 0009 §2）。

inbound provenance は、少なくとも以下を表現できる余地を持つ。

- actor（発話者）
- surface / place
- Workspace（Workspace channel の場合。global DM には存在しない）
- message_id / seq
- 解決済みの mention / addressee
- trigger reason / urgency
- correlation / causation
- admission 時の authority

後方互換は取らない。`DirectChatProvenanceV1` を surface 一般の
`InboundProvenanceV1` へ置換する（pre-launch contract replacement、
ADR 0008 §2）。

**「reset」の範囲を限定する。** 同じ `PersonalityAgentId` を保ったまま人生ログ
や command 行を消すことは、ADR 0008 の定義では **death** である。したがって
reset してよいのは次に限る。

- 合成 fixture（`contracts/agent-events-fixtures.json`、テスト内の固定値）
- 誰の人格でもない使い捨ての dev 個体。**新しい `PersonalityAgentId` で
  再 provision する**（消すのではなく、別の個体を作る）

保持する個体は、旧 provenance 行を一度だけ移行する。移行を書かないなら、その
個体は再 provision するしかない。どちらを選ぶかは個体ごとの判断であり、
「dev だから消してよい」という一般則は置かない。

**Postgres の戸籍・auth・agent secrets（`apps/api/internal/db/migrations/`
0001–0004）は reset の対象外である。** identity 台帳は provenance の形が
変わっても不変であり、HumanId / PersonalityAgentId の同一性はそこに依存する。

### 3. agent も場所を開く。閲覧と送信は同じ状態の中で起きる

人間は Discord で「取得」してから「送信」するのではない。channel を開き、
その画面で読み、同じ画面で書く。閲覧と送信は独立した二つの操作ではなく、
**その場所にいるという一つの持続的な状態**の中で連続して起きる。これが
人間の認知モデルであり、messaging の UI がその形をしているのは偶然ではない。

したがって人格 agent の道具を、stateless な CRUD 動詞の並び
（`fetch` して `send` する）として設計しない。それは窓口へ書類を出しに行く
形であり、同じ場所に居合わせる同僚の形ではない。動詞の分解自体が bot 的な
使い方を強制する。

人格 agent の道具は、人間が画面で行うことと同じ構造を持つ。

| 人間が画面ですること | agent の道具 |
| --- | --- |
| サイドバーを見渡す（どこに何件、mention はどこか） | 場の一覧と未読の見出しを見る。開かずに、いつでも |
| channel / DM を開く、別の場所を開く | 場所を開く。直近のタイムラインが見え、開いた状態が続く |
| 上へスクロールして遡る | 開いている場所を遡る |
| 開いている画面の composer に打つ | 開いている場所へ書く（返信先・緊急度・mention は書く行為の一部） |
| 見えているメッセージにリアクションする | 同上 |
| 見えているメッセージに「後で返信します」を付ける | 同上 |
| 自分の発言を編集する、取り消す | 同上（取り消しは tombstone。消した事実と seq は残る） |
| ステータスを自己申告する | 場所によらず行える |
| 検索窓を開き、結果からジャンプする | 検索し、結果の場所を開く |

この表は web の UI と同時に更新される。片方に操作が増えて他方に無い状態を
残さないことが、§5 の agency 対称性の担保である。

**できるだけ同じ形にし、agent にとってより適した方法があるときだけそちらで
代替する。** 人間のスクロールは画面サイズと目の制約から生まれた身体動作で
あり、agent にスクロールは無い。「過去へ遡る」という同じ能力を、agent は seq
範囲の指定で果たしてよい。揃えるべきは能力であって身体動作ではない（§5）。
代替は能力を保ったままの言い換えに限り、増減を伴わない。**AX と UX が高い
精度で一致していることが、この設計の目的である。**

この構造から従う含意:

- **見えていないものは操作できない。** リアクションも「後で返信します」も、
  どの view でも開いていない場所のメッセージには付けられない。permalink を
  踏めばその場所が開き、そこで操作する。人間と同じ順序である。
- **既読は本人が押すボタンではない。** ただし read cursor の前進条件は
  §6 の制約に従う。
- **見渡すことは常にできる。** 人間のサイドバーは、起こされ方に関わらず起きた
  瞬間からそこにある。誰かが渡すのではなく、見ればある。agent も同じで、
  覚醒理由によって見える範囲が変わることはない。何を添えて起こされるかは
  本人の設定であり（§8 (2)）、添えられなかったものも自分で見に行ける。
- **開いている場所の新着は流れ込む。** ただし注入経路は §8 の境界を通る。

要約・キュレーション・重要度判定・自動引用は **道具ではなく仕事** であり、
本人の判断に属する。人間が同じことをしたければ自分で読んで自分で書くのと
同じく、agent もこの道具だけを使って自分で行う。

### 4. view は人格ではない。一人であることから一 view は導けない

人間は一人でも、複数の窓・端末・アプリを開く。人格の一意性から、表示中の
view が一つであることは導けない。次の三つを分離する。

| | 個数 |
| --- | --- |
| PersonalityAgent（人格、agent session、人生ログ） | 1 |
| MessagingView / ClientConnection | 0..N |
| 各 view の focused place | 0..1 |

MVP として、一つの messaging client view が一度に一つの place を focus する
のは構わない。しかしそれは client の実装都合であり、**人格 agent 全体の
canonical な attention 状態ではない**。「今どこにいるか」を本人の唯一の状態
として domain に固定しない。

### 5. 粒度は「人間が一息にやること」に合わせる

同型性を動詞の細かさへ短絡させると、provider への往復が無用に増える。基準は
細かさではなく、**人間が画面で一息にやること**である。それは 1 回で送れる。

- **場所を開くとは、画面を見ることである。** 直近のタイムライン、未読位置、
  参加者、自分宛の mention は一度に返る。画面を見れば全部見えるものを、
  分割して取りに行かせない。
- **書くとは、composer で一度に決めることである。** 本文・返信先・緊急度・
  mention・添付は一つの操作であり、一度に送る。
- **逆に、間に判断が挟まるものは分ける。** 読んだ結果を見て何を書くか決める
  以上、読むと書くを一つに畳めば「読まずに書く」ことになる。人間も同じであり、
  ここを畳むのは効率化ではなく別の振る舞いへの変質である。

複数の場所を見て回ることは、一つの turn の中で複数の tool call を並べて
行える。既存の run は、一つの assistant turn が返した複数の tool call を
順序どおり逐次実行し、まとめて provider へ返す
（`tool_calls_execute_strictly_sequentially_and_continue_provider`）。これは
provider 非依存の既存機能であり、messaging のために新設しない。ただし
**複数 tool call 対応は持続的 view の代替ではない**。往復を減らす手段で
あって、状態を持たない設計を正当化しない。

**揃えるべきは domain 上の agency であって、内部実装ではない。** human と
人格 agent で同型にすべきなのは次の四つである。

- 同じ通知設定 resource を所有できる
- 同じ event を受け取れる
- 同じ権限で open / reply / defer 等を選べる
- 通知 service に認知判断を奪われない

subscription、ack、cursor、ページングは内部契約として必要であり、「人間の UI
に無い動詞だから」という理由で禁じない。禁じるのは agency の非対称であり、
人間には与えず agent にだけ与える能力（またはその逆）を作らないことである。

逆に、**human の脳と agent runtime の内部実装まで同一にする必要はない。**
内部実装を無理に揃えると、かえって「人間用 GUI を逐語的に模倣する bot」へ
戻る。§3 の対応表は affordance の対称性を担保するためのものであり、実装の
逐語移植を指示するものではない。

### 6. 既読は本人が押すボタンではないが、read cursor は durable admission 後に進む

人間は既読ボタンを押さない。開いて見れば進む。したがって agent 向けの tool に
`mark_read` を露出しない。

しかし **開いた瞬間に read cursor を進めてはならない**。agent が timeline を
受信してから人生ログへ durable に取り込む前に落ちると、本人が経験していない
メッセージが既読になり、再配送されなくなる。

- 内部 protocol には idempotent な `read_through(place, seq)` を残す。
- agent は内容の durable admission 後に ack する。
- web も実際に表示した範囲まで read cursor を進める。

UX 上の自動既読と、wire 上の確認操作は両立する。前者は本人が押す動詞が
存在しないという話であり、後者は「経験した」ことの確認である。

**「経験した」の条件は client の性質によって異なる。** agent は人生ログへの
durable admission が条件であり、web は画面へ実際に表示したことが条件である。
web は人生ログを持たないため、両者に同じ判定を課さない。共通なのは、
受け取っただけでは進めないという一点である。

### 7. 閲覧は presence を生成しない

Discord でも、誰がどの channel を閲覧中かは通常ほかの参加者に見えない。
**読むことと、在席を表明することは別**である。次を分離する。

| | 性質 |
| --- | --- |
| focused / open place | private な client state。他者に公開しない |
| online / status | 明示的、または独立した presence resource |
| typing | ephemeral な明示 event |
| ReplyLater | 本人が選んだ durable event |

「その場にいる」表現を将来導入することは可能だが、閲覧から自動的に公開
presence を生成しない。

### 8. 境界が持つのは権限と安全だけで、起こされるかどうかも注意も本人が決める

**起こされるかどうかを決める権利は本人にある**（非通知モード）。以下で
「決定論的境界」と呼ぶものが持つのは、権限と安全——membership / authority、
相手側の block、rate limit——だけである。「起こすべきか」はそこに入らない。
境界がしているのは判断ではなく、**本人が先に置いた指示の実行**である。受付が
電話を取り次がないのは受付の判断ではなく、本人がそう言ったからである。

開いている場所の新着がリアルタイムに流れ込む体験は採る。しかしそれを
**現行の soft steer へ直接注入しない**。現行の soft steer は汎用の messaging
stream ではなく、実行中の run へ `UserMessage` を注入する制御である。異なる
actor の全発言をそこへ流せば、通知設定と attention 判断を迂回し、すべてを
provider 上の user message として扱うことになる。

前提として、**本人と provider 呼び出しを同一視しない**。provider 呼び出しは
本人の認知活動を実行する一手段である。本人が事前に選んだ規則、private
runtime 上の attention arbiter、軽量な推論、現在の main run での判断のいずれも
本人の注意機構を構成し得る。「本人が判断する」は「毎 event ごとに main
provider を呼ぶ」を意味しない。

そのうえで、配送と注意の境界を次の段階に分ける。

**(1) Delivery eligibility — 決定論的な境界。** shared messaging / control
plane が評価するのは、明示された設定と安全・認可だけである。評価するものは
二種類あり、由来が違う。

*境界自身の権限で決まるもの（安全と認可）*

- membership、authority、privacy
- 相手側の block
- rate limit、重複排除、短時間の coalescing

*本人が先に置いた指示（境界はこれを実行するだけで、判断はしていない）*

- mute、非通知モード、通知設定（§9）
- quiet hours / scheduled availability
- mention、direct call、urgency 等の明示 signal をどう扱うか

**ここで本文の意味を解釈して「重要だから割り込ませる」と判断しない。**
明示的な通知設定は本人が事前に行った選択であり、その規則を決定論的に適用する
ことは agency の喪失ではない。逆に、本人が置いていない規則を境界が足すことは、
それがどれほど親切に見えても agency の喪失である。

**(2) AttentionCandidate — 本人の private runtime へ渡す。** eligibility を
通過した event は、現在の provider turn へ直接注入せず、typed な候補として
渡す。少なくとも surface / place、event または message reference、unread
range、actor / mentions、trigger reason、declared urgency、arrival time、
correlation / causation を参照できる形とする。

**候補に何を添えるかは本人の設定である。** 最小限の参照だけを受け取ることも、
認可済みの snapshot（未読の見出し、place 一覧など）を最初から添えて受け取る
ことも、本人が選ぶ。まとめて見る道具も細かく見る道具も両方用意し、いつ
どちらを使うかも本人が決める。**自動で添えること自体は禁じない。禁じるのは、
それを本人以外が決めることである。** 呼ばれたときに資料も一緒に持ってきて
ほしい人もいれば、呼ばれただけで十分な人もいる。

**この段階で read cursor、presence、focused place を変更しない。** 候補が
届いた状態、あるいはそれによって起きた状態は、**通知に気付いた**状態である。
その place を開いた、既読にした、presence を出した、のいずれでもない。人間が
通知に気付いてから開くまでの間にある判断を、システムが先回りして消費しない。

**(3) Wake decision — 明示設定から決定可能な範囲だけ外側で行う。** 停止中の
agent はそのままでは判断できないため、起動の可否は外側の gate が担う。ただし
gate は意味的重要度を独自に判断せず、本人が設定した規則を実行する（direct
call は起こす、mention は起こす、muted place は scheduled review まで溜める、
quiet hours 中は emergency grant 以外を溜める）。**曖昧な event を勝手に
棄却しない。** durable queue へ置き、次の自然な覚醒または scheduled review で
本人へ見せる。

**(4) Attention allocation — 本人が判断する。** 起動済みの本人が、現在の活動
を interrupt する / safe boundary で現在 context へ取り込む / view を開いて
観察する / 後で見る / 明示的に無視する、を選ぶ。**messaging service や通知
設定 service が本人の代わりに確定しない。**

ここでの「後で見る」（defer）は本人の注意配分であり、誰にも見えない。相手に
見える durable marker である ReplyLater（§7）とは別のものである。後者は本人が
場を開いて明示的に置く表明であり、defer から自動的に生成されない。

**(5) コスト最適化は認知機構の実装として扱う。** すべてを main provider へ
渡す必要はない。本人が設定した決定論的 rule、debounce / batching、private
runtime 上の軽量 attention arbiter、小さい model による曖昧候補の整理、main
provider による高価値・曖昧・不可逆な判断を、段階的に使える。

**軽量 arbiter は別人格でも本人の分身でもない。** 人間が通知設定・習慣・
秘書・フィルタを使うのと同様に、本人が使う attention subsystem であり道具で
ある（ADR 0008 の「tool は本人ではなく本人が使う道具」の適用）。判断結果には
actor、根拠となった設定、候補、選択、時刻を監査可能な形で残す。model や
effort は domain contract へ固定せず、同じ AttentionCandidate 契約の内側で
交換可能にする。

**soft steer との接続。** AttentionCandidate をそのまま `WireCommand::UserMessage`
へ変換しない。現在 run がある場合でも、まず typed な候補として safe boundary
へ通知し、本人が inject を選んだ場合にだけ、actor と surface provenance を
保持した入力へ変換する。異なる人々の発言を無差別に provider の user role へ
混ぜない。

全体の順序は次のとおりである。

```
delivery eligibility（境界の安全・認可 + 本人が置いた指示の実行）
  → durable AttentionCandidate
  → agent-owned attention decision
  → interrupt / inject / defer / observe
  → 必要なら view open と durable admission
  → read_through
```

**リアルタイムに画面へ流れ込むことと、今の model turn へ即注入することは
別である。** 開いていない場所の発言を agent session へ流し込まないのは、
runtime が生きている間も同じである。これは人間が、見ていない画面の発言を脳へ
直接流し込まれずバッジだけが増えるのと同型であり、同時に 3 層メモリの汚染と
コストを防ぐ。

view の buffer は runtime が生きている間だけ存在する（§12）。ランタイム停止中
に届いた発言に「現在の view」は無く、(1) を通過したものは (2) の候補として
durable queue に入り、(3) の規則が起こすか、次の自然な覚醒まで待つ。

### 9. 通知設定は agent 本人の持ち物である

channel ごとの mute、mention のみ、全件といった通知設定 resource は human と
人格 agent で同型とし、本人が自分で設定する。**起こされるかどうかを決める
非通知モードもここに含まれる。**

**Employer の「時間帯」と本人の quiet hours は別物である。** ADR 0010 §3 の
Employer 設定は **自律的な衝動**——本人が自分から動き出すこと——の許可・予算・
時間帯であり、コスト責任に属する。呼びかけで起こされたくない時間は本人の
指示であって、Employer のものではない。前者は「勝手に働き始めてよい枠」、
後者は「呼ばれても出ない時間」で、重なることはあっても同じものではない。

Employer の予算が尽きているときは、本人が「起きたい」と設定していても起きられ
ない。これは本人の指示の否定ではなく、**資源の可用性**である（交通機関が止まって
いれば出社したくても出社できない）。この場合も候補は durable queue に残り、
資源が戻ったときに本人が見る。**予算切れを理由に候補を捨てない。**

「本人の持ち物」は所有権の話であり、保管場所の話ではない。設定を書き換え
られるのは本人だけだが、**保管と評価は shared messaging service 側に置く**。
評価が起きるのは配送の手前であり、そこで読めなければ「起こさないで」を確認
するために起こすことになる。agent DB が持つのは projection である（§10）。

### 10. 共有 domain data の正本は Workspace API 側に置く

message、place、membership、既読、自己申告 status の正本は control plane に
ある。agent DB が持つのは projection と local copy であり、source provenance・
取得時の authority・revocation/refresh・retention に従う（ADR 0008 §8）。

### 11. 一人の agent は一つの人生ログを持つ

人格 agent は全 Surface を通じて一人であり、人格を形成する agent session /
人生ログも一つだけである。place、channel、DM、direct chat ごとに session や
人格を分裂させない。複数の place から呼びかけられても、**すべて同じ本人が
所有する一つの durable candidate queue に入る**。session の入力になるのは、
本人が inject を選んだ後だけである（§8 (2)(4)）。「一つの人生」と「一つの
入力キュー」は別のことで、後者に短絡させると全 channel 発言が provider turn
へ流れ込み、channel bot へ退行する。

並行して届いた呼びかけをどう捌くかは本人の判断であり、runtime による分身では
解決しない（ADR 0008 の「同じ人格の独立 continuation を並列起動する」の棄却）。

### 12. runtime 停止をまたぐ状態の境界

| Durable | Ephemeral |
| --- | --- |
| read cursor | WS subscription |
| 通知設定・非通知モード | focused / open 状態 |
| ReplyLater 等の約束 | typing |
| 最後に閲覧した place（private な client preference） | online / connection presence |
| 自己申告 status（TTL 付き） | — |
| AttentionCandidate queue と本人の cursor | — |

**自己申告 status と connection presence を混同しない。** 前者は本人が置いた
表明であり、runtime が落ちても `expires_at` まで残る（「取り込み中」と言って
席を立った人の札は、その人が居なくても掲げられたままである）。後者は接続が
生きている間だけの事実であり、切れれば消える。

**AttentionCandidate の lifecycle 契約は本 ADR では未定である。** 正本の所在
（shared control plane か agent-private runtime か）、candidate id、ack と
cursor、再配送、runtime generation fence を確定しないと、runtime 停止と再接続
で呼びかけの欠落または重複判断が起きる。これは arbiter の実装詳細ではなく
messaging service と agent runtime の境界そのものなので、接続契約の凍結時に
確定させる（Open questions）。

再起動時に、最後に閲覧した place を UI 上の便宜として再度開くことはできる。
ただし新しい connection として再認可・再 subscribe する。停止前から
「そこに居続けていた」ことにはしない。これは人間がアプリを開き直したときに
前の画面へ戻るのと同じであり、その間ずっと在席していたことにはならない。

## Consequences

- `contracts.rs` の `DirectChatProvenanceV1` / `HumanActorProvenance` は
  surface 一般の `InboundProvenanceV1` / actor へ置き換わる。既存の direct
  chat wire は破壊的に変わる。合成 fixture は reset し、保持する個体は一度だけ
  移行するか新しい id で再 provision する（§2）。
- `HumanActorProvenance` の `principal_id` は自由文字列の検証しか持たない。
  canonical `HumanId`（UUIDv7）へ寄せる必要がある（§2）。
- `apps/agent/src/apiclient` に Workspace API クライアントの実装が必要になる。
  現在は空ファイルである。
- `ToolCtx` は `WorkspacePaths` しか持たないため、Workspace API を叩く tool を
  受け入れる拡張が必要になる。tool の risk 分類（送信は取り消せない発話で
  あること）も設計対象となる。
- messaging の道具は stateless ではない。view の focused place という状態が
  存在するが、それは client state であって人格の canonical な状態ではない
  （§4）。
- §8 は三つの新しい構成要素を要求する。shared 側の決定論的な delivery
  eligibility 評価、durable な `AttentionCandidate` キュー、本人の規則だけを
  実行する wake gate である。いずれも soft steer とは別の層であり、
  `WireCommand::UserMessage` への変換は本人が inject を選んだ後に限られる。
  wake gate は ADR 0010 の覚醒トリガと同じ場所にある。
- 本 ADR は §8 の順序と境界のみを定める。arbiter の構成・model 選択・
  debounce 戦略は別 Decision で扱うため、Decision 1・2 の実装はこれを待たない。
- `apps/api` に `/messaging/…` REST と `/messaging/ws`（1 本 multiplex）が
  必要になる。web 側の `MessagingBackend` インターフェースはこの差し替え点
  として既に切られている。read cursor の ack、subscription、ページングは
  この内部契約に含まれる（§5）。
- web 側のモックサーバーが模擬している人格 agent の振る舞い（FYI には
  リアクションのみ、取り込み中は「後で返信します」）は、本 ADR 適用後は
  実装ではなく **本人の判断** になる。モックはあくまで UI 検証用であり、
  そこに書かれた振る舞いを仕様として実装側へ持ち込まない。

## Rejected alternatives

### channel ごとに agent の session / run を生やす

throughput と分離の点では素直だが、一人の主体を place 単位の worker へ
分解する。ADR 0008 の棄却済み案と同じ形である。

### 閲覧と送信を独立した stateless 動詞へ分解する

`fetch(place)` で読み、`send(place, text)` で書く形は REST として自然で、
実装も単純である。しかし人間は画面を開いて読み、同じ画面で書く。分解された
動詞は、毎回窓口へ書類を出しに行く形を agent に強制し、それは同僚ではなく
bot の使い方である。動詞の設計がそのまま振る舞いの設計になるため棄却する。

### 一 place 制約を人格の canonical な attention 状態にする

本 ADR の初稿は、人格が一人であることから「一度に一つの place しか開けない」
を導いた。しかし人間も一人で複数の窓を開く。view の個数は client の都合で
あり、人格の一意性とは別の階層である（§4）。

### 開いた瞬間に read cursor を進める

「開けば既読」は UX として正しいが、wire 契約としては危険である。受信から
durable admission までの間に落ちると、本人が経験していないメッセージが既読に
なり再配送されない。UX 上の自動既読と wire 上の ack を混同しない（§6）。

### 閲覧から公開 presence を自動生成する

初稿は「隠れて読む経路を作らない」として閲覧を可視化したが、これは Discord の
挙動としても誤りであり（誰がどの channel を見ているかは他人に見えない）、
読むことと在席の表明を混同している（§7）。

### 開いている場所の新着を soft steer へ直接注入する

現行の soft steer は実行中の run へ `UserMessage` を注入する制御であり、汎用の
messaging stream ではない。全発言をそこへ流すと通知設定と attention 判断を
迂回する（§8）。

### 覚醒時に place を open 済みにする

往復は減るが、「通知に気付く」と「開く」の間にある本人の判断を消費する。
place と unread range を context として渡すことと、開いたことにするのは
別である（§8 (2)）。

### 要約・重要度判定・自動引用を messaging API の動詞にする

agent の仕事を product feature として規定する行為であり、同型性を壊す。
必要なら本人に頼めばよく、その依頼は既存の道具（読む・書く・引用する）で
遂行される。

### agent を webhook / bot integration として接続する

外部システム連携の形式で本人を繋ぐと、identity が integration 側に生まれ、
戸籍（ADR 0009）から切り離される。将来の外部 Surface 連携は、本人が
外部アカウントを使う形（本人の道具）として別途設計する。

## Non-goals

- 通知の配送実装（メール、push、モバイル）。本 ADR は覚醒トリガの発行までを
  対象とする。
- モデレーション、権限マトリクス、招待・フレンド申請のワークフロー。
- 外部 Surface（Discord 等）との連携。
- 人格 agent の自律的な発話（衝動由来の投稿）の予算・時間帯制御。
  ADR 0010 §3 の Employer 設定に従うが、具体的な制御の実装は別途。
- 「その場にいる」ことの可視化（§7）。将来導入し得るが本 ADR では扱わない。
- §8 の attention arbiter の具体的な実装。本 ADR は契約の境界と順序のみを
  定める。

## Open questions

- **`AttentionCandidate` の lifecycle の正本をどこに置くか。** shared control
  plane を正本とし per-agent cursor で読ませるか、private runtime へ
  transactional に handoff するか。runtime 停止中に届いたものを受け取れるのは
  shared 側だけなので前者が有力だが、確定していない（§12）。candidate id、
  ack、再配送、generation fence をあわせて凍結する必要がある。
- **配送に関わる四つの境界の所有関係。** messaging backend、notification
  service、Employer の資源 gate、agent-private な attention。§8 は「境界が
  持つのは権限と安全だけ」と定めたが、quiet hours や dedupe を実際にどの
  service が保持・評価するかは、本 ADR と契約ドラフトで記述が割れている。
  一つに確定しないと shared service が本人の注意判断を持つ形になる。
- **新規 agent の通知設定の初期値を誰が置くか。** 本人がまだ設定していない
  provisioning 時点の default は product policy として認めてよいか。人間の
  新入社員にも既定はあるので認めてよいと考えるが、(a) 本人が最初に見た時点で
  変更できること、(b) 既定が「起こす方へ倒す」ではなく常識的であること、が
  条件になる。
- 人格 agent が place の membership を持つ単位。契約ドラフト v0 は
  「channel は Workspace 直下、active メンバー全員が閲覧・投稿可」で確定して
  いるため、この問いは「将来 place 単位の join を導入するときの単位」に狭まる。

**解決済み。** `/messaging` の実装担当は決まった（messaging microservice と
配送は Claude/Fable、agent 側の Surface 契約・AttentionCandidate・注意の経路は
Codex）。送信の `ToolRisk` は独立した設問ではなく、Workspace の権限モデル
（role・membership・place の可視性）に乗る。道具が知るのは「権限が無ければ
失敗する」ことだけで、誰が投稿してよいかを道具が判断しない。人間が制御できる。
