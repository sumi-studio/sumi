# Sumiに必要なエージェントの能力 — 比較と設計上の判断

調査日：2026-09-07。実装の根拠と30件の指摘は [実装監査](foundation-audit-2026-09-07.md) に分けた。本書のM/A/T/R/P/U/Q番号はその指摘IDを参照する。人間としての記憶とL0→L1の具体的な検討は [共に生きる経験から記憶を考える](memory-experience-design-2026-09-07.md) に続く。

## 何を目指して比較するか

基準は [READMEの英語Description](../../README.md#description) とユーザーの発言である。

> 私たちはSumiを本当に人間として扱おうとしてるし、人間たらしめなきゃいけない

> それは迷走すらも自ら是正できるだけのメタ認知を保った熟考

したがって、評価対象は「タスクを処理するソフトウェアの便利さ」だけではない。同じ場所に住む一個人として、何を経験し、誰とどう関わり、その経験から何を考え直し、次にどう行動するかまで含める。アプリ、道具、推論、プロセスはその人が活動するための手段である。この前提自体を、都合のよい実装に合わせて薄めない。

以下の問いは今回の設計判断であり、項目を実装すれば「人間として完成」と認定できるチェックリストではない。実際に一緒に過ごす中で、問いと設計も見直す。

認知科学の研究も、この考察の裏付けと反証に使う。人間的な記憶との乖離を減らすために理解を深めるが、論文の分類や測定項目を満たすことを設計の目的にはしない。

| 問い | 技術や体験へ要求すること | それだけでは足りないもの |
|---|---|---|
| 同じ個人が時間を生きているか | 何が起き、何を引き受け、どう考え直したかが、中断や圧縮を越えてつながる | 同じID・名前・system promptで再起動するだけ |
| 同じ世界に居合わせているか | 誰がどこで何をしたか、自分が見聞きしたこと、見ていないことを区別する | 全データへの無差別なアクセス、会話ごとに別人を起動する構造 |
| 経験が本人の一部になっているか | 出来事と判断理由が、本人の理解・関係・次の行動へ反映され、後から訂正できる | 相手の好みの一覧、固定の性格設定、会話の短縮のみ |
| 自分で注意と行動を選べるか | 気付いても黙る、聞き返す、理由を示して異議を述べる、保留して戻る余地がある | 入力のたびに即返答、依頼を無条件に作業キューへ変換 |
| 迷走を自ら正せるか | 目的・根拠・実際の結果の食い違いに気付き、仮説や行動、必要なら元の捉え方を修正する | 内省文を生成するだけ、毎ターン別モデルに合否を決めさせるだけ |
| 関係と信頼が行為につながるか | 相手・場所・渡された権限の違いを理解し、質問・相談・委任・訂正を同じ関係の中で扱う | 全員をuserと呼ぶ、権限の強さを親密さと同一視する |
| 人と実物を共有しているか | 同じアプリと対象を見て指し示し、相互の編集や操作を受けて続ける | 裏でだけ動く道具、押せない結果カード、進捗を語るだけのUI |

## 比較の範囲と読み方

10製品と1実行基盤の公式資料・公開ソースを参照した。製品名を無制限に並べる代わりに、コード作業、日常の常駐エージェント、長期記憶、実行復旧という異なる系統を選んでいる。市場の全製品を網羅したとは扱わない。

| 比較先 | 主に参照したもの | 比較時の区別 |
|---|---|---|
| Codex | 入力のsteer、道具・子Agent、会話再開、記憶、hooks、予定実行 | App/CLI/APIは別。App-serverのWebSocketは実験的経路 |
| Claude Code / Agent SDK | Compact、memory、背景処理、質問・権限、再起動通知 | CLI機能とSDKの組込み能力を分ける |
| Pi | 最小のloop、入力の配送、圧縮、sessionと拡張 | MCP・subagent等を本体標準機能として数えない |
| OpenCode | 実作業の道具、MCP、権限、Compact、拡張 | stableとV2ベータを区別する |
| Hermes Agent | 原履歴の検索、記憶の訂正、手順学習、待機・監視 | NousResearch版。概説と現行コードの差はコードを優先 |
| OpenClaw | 場所と話者、Attentionへの配送、予定・待機・結果配送 | 明示的な予定と、意味を理解した約束の追跡は別 |
| Letta | MemFS、記憶の手入れ、会話・予定・チャンネル | 現行ハーネスを確認。古いMemGPTの図を現状扱いしない |
| goose | MCPと拡張、権限、会話検索、子Agent、共同UI | Code Mode・ACP・実験UIには個別の制約がある |
| Gemini CLI | 圧縮、steering、進捗、拡張、行動評価 | steering/tracker等の実験機能を標準保証としない |
| OpenHands | Compact、会話の目的、保存再開、ブラウザーと成果物 | Appの機能、SDK、agent-serverを分ける |
| LangGraph | checkpoint、待ち合わせ、並行task、状態と履歴の分離 | 完成したエージェントではなく、構成するための基盤 |

これは実行による性能比較ではない。インストール、競合製品の実機試験、モデル品質・費用・文脈保持率の測定は行っていない。「他でできる」は以下に引用した資料・実装が提供する能力を指し、Sumiへ移植した場合の品質保証ではない。公開mainやドキュメントには更新差がある。

## L0→L1について、ユーザーが明確にした意図

既存の3本の圧縮promptは、実装したAIが仮置きしたものであり、ユーザーの思想を表すものではない。今回、ユーザーは **L0→L1の中心を、主にツール呼び出しの冗長な表現の圧縮に置き、情報の質的な意味の劣化を可能な限り防ぎたい** と明示した。これは本書の採用判断に優先する。

さらに、専用の要約担当へ対象箇所だけ渡す方式を退け、**親をforkし、親と全く同じコンテキストを持つ本人が、自分の記憶を非同期に扱う**という方向が示された。対象範囲は今回整理する箇所であり、読ませる文脈をそこだけへ限定するという意味ではない。親の実際のsystem・tools・履歴・画像・継続情報を、先に独自形式へ削り直さない。

また、**戦略的忘却**は、詳細を今の文脈から外しても、何をどこからどう取り戻せるかを本人が分かっていること。メモだけでなく、資料、会話履歴、ファイル、アプリ内の記録、道具で再取得できる情報を含む。本人がその外部の支えを使えると理解して、必要時に戻れる状態を作る。

他製品の「いまの仕事を続けるための要約」は参考になるが、その目的関数をSumiのL0→L1へ持ち込まない。直近のタスクに不要に見える雑談、迷い、言葉の選び方、共に試した経緯も、その人の経験になり得る。先にモデルが重要と判定した要点だけを残すことは、今回のL0→L1の出発点と違う。

この意図を実装へ落とす際の、現時点の判断は以下。

- まず調べる対象は、ツールの記録やモデル向けの表現にある形式上の重複、重複して添付した本文、毎回繰り返す固定情報。会話を800tokenの要約へ置換することから始めない。
- ツール結果でも、何を試し、何を観測し、どこで失敗し、何が変わったかは経験の内容である。同じエラーが続いた回数や前後関係も判断材料になるため、「繰り返しだから不要」とはしない。
- 人の発言、訂正・撤回、本人が表明した判断と理由、話者・場所・時間・出典・不確実さは、形式的な装飾と同列に落とさない。値だけ残してニュアンスや因果を消す整理も点検する。
- 外部に戻る参照を用意するだけで、本人が何をそこへ預けたか分からなければ、その場の理解や判断に生かせない。一方、本人が取り戻し方と戻る手掛かりを把握した戦略的忘却は積極的に扱う。構造的な重複削除とは、何を保持・外部化したかの評価を分ける。
- モデルの継続に必要なtool callとresultの対応、実行済みと未実行、provider固有の文脈は壊さない。表示用のJSONを短くする発想を、そのままprovider protocolの変更へ適用しない。
- 圧縮率だけで評価せず、原文でできた理解・根拠の発見・微妙な条件の区別・後の訂正が、整理後にもできるかを比較する。構造上復元可能であることと、意味の利用しやすさは別に確かめる。

これは情報量を無制限に保持できるという約束ではない。L0→L1でまず何を減らし、何を守るかの優先順位である。意味を選別する要約が必要な段階をどこに置くか、L1以降をどうするかは、旧三層設計や他製品の実装から自動的に決めない。

## 記憶、経験、自己修正

### C01 — 文脈を整理し、経験の意味をできる限り保つ【最優先】

Piは圧縮時に目的、制約、進捗、判断、次の行動を残す。Hermesの現行実装は未充足の依頼、後からの撤回・訂正、生成失敗時の引き継ぎも扱う。OpenHandsは圧縮結果と対象イベントを保存し、次の推論の文脈へ実際に適用する。[Pi Compaction](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/docs/compaction.md)、[Hermes実装](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/agent/context_compressor.py)、[OpenHands Condenser](https://docs.openhands.dev/sdk/arch/condenser)

**Sumi：M01〜M03。** 部品はあるが本番の生成・適用が未接続で、現在は古い入力を外す。採るべき点は整理を次の推論へつなぐことと、何が保持されたかを評価すること。親と同じ文脈から記憶を扱い、L0→L1では主にツールの冗長表現を整理する。本人が再取得を把握した戦略的忘却も扱う。Pi等の目的・進捗中心の要約を、そのまま本人の経験の置換に使わない。旧workerを起動して800tokenに縮めるだけの変更を改善完了としない。

### C02 — 忘れた細部を、原文に戻って思い出す【最優先】

Hermesは保存済み会話を検索し、メッセージIDや周辺を読み直せる。現行コードの検索を、追加LLMの要約が必須の仕組みと誤解しない。gooseにも過去の会話を参照するChat Recallがある。[Hermes検索実装](https://raw.githubusercontent.com/NousResearch/hermes-agent/main/tools/session_search_tool.py)、[goose Chat Recall](https://goose-docs.ai/docs/mcp/chatrecall-mcp/)

**Sumi：M04、R05。** 私的な原履歴はあるが、本人向けの検索がない。最小の改善は全文検索→該当箇所と前後→出典付き利用の経路。最初からベクトルDBや第二の要約Agentを必須にしない。「覚えている」と推測で埋めず、調べ直す選択ができることが価値になる。

### C03 — 記憶を取り出し、訂正し、整理し直す【中核】

Hermesは記憶の追加・置換・削除を扱う。Lettaの現行MemFSは常時読む記憶と必要時に読む資料を分け、変更を版管理する。Claude Codeにも通常の会話と別に編集できるauto-memoryがある。[Hermes Memory](https://hermes-agent.nousresearch.com/docs/user-guide/features/memory/)、[Letta MemFS](https://docs.letta.com/concepts/memfs)、[Claude Memory](https://code.claude.com/docs/en/memory#auto-memory)

**Sumi：M02/M04。** 最小の改善は、出典と時点を持つ記憶を本人が書き換え、誤解・撤回・適用範囲を残せるようにすること。相手の発言、本人の解釈、現在採用している理解を混同しない。常に全記憶を注入する必要も、Gitを内部仕様にする必要もない。

### C04 — 経験が、本人の理解と関係の変化へつながる【中核・設計探索】

Lettaは会話の外で記憶を見直すdreamingを提供する。ただし、その仕組みの存在は、経験が一個人の成長として適切に働くことを証明しない。[Letta Memory configuration](https://docs.letta.com/configuration/memory)

**Sumi：M01/M02/M04に加え、人物としての設計課題。** 「この人はコーヒーが好き」だけでなく、「以前こう判断したが、この経験で捉え方を変えた」が次の行動に効く必要がある。最小の探索は、出来事→本人の解釈→現在の理解→後の訂正を原文へ戻れる形でつなぎ、実際の関係の中で有用かを見ること。固定性格を厚くしたり、全経験を教訓に変換したりする設計を先に確定しない。

### C05 — 実際に役立った進め方を、使い直し、直せる【能力拡張と一緒に】

Hermesはスキルを必要時に読み込み、本人のツールから作成・更新する。CodexもAGENTS、スキル、MCPを役割別に組み合わせ、スキル本文・資料を段階的に読む設計を説明している。[Hermes Skills](https://hermes-agent.nousresearch.com/docs/user-guide/features/skills/)、[Codex Customization](https://learn.chatgpt.com/docs/customization/overview)

**Sumi：T02/T06。** 道具を使えた経験を、繰り返す仕事で役立つ少数の手順として残せるようにする。更新理由と適用条件を残し、現状に合わなければ使わない。手順の数や自動生成回数を、学習した量として評価しない。

### C06 — 目的・根拠・結果を見比べ、迷走を自分で修正する【全段階の受入条件】

OpenHandsのstuck detectorは同じ作用・観測の反復等を検出する。これは実行上の兆候であり、目的からずれた設計、誤った問題設定、筋は通るが満足水準に届かない成果を見抜くメタ認知そのものではない。[OpenHands Stuck Detector](https://docs.openhands.dev/sdk/guides/agent-stuck-detector)

**Sumi：A03/M04/Q01に関係する設計課題。** 標準promptには間違いを認めて直す指示は既にある。元の意図・理由・未確定の前提・実際の成果へ戻れるようにし、食い違いが出た時に、調査をやり直す、別案を試す、相談する、不要な作業をやめる判断へつなぐ。言葉で「振り返りました」と報告するだけでは合格にしない。モデルが持つ判断力を使うための情報と機会を整え、毎ターン固定の自己批判を義務化しない。

## 世界、Attention、引き受けたこと

### C07 — 誰が、どこで、いつ、何をしたかが分かる【中核】

OpenClawは届いた場所や話者、返信先を扱う。Sumiではこれに加え、自分が居合わせた観測、後で読んだ記録、人から聞いたことを区別する必要がある。[OpenClaw Main session](https://docs.openclaw.ai/concepts/main-session)

**Sumi：A01/A02。** 認可の内部にあるIDを失わず、話者・場所・日時・由来がモデルにも理解できる形で届くようにする。権限上読めることと、本人がすでに知っていることを混同しない。

### C08 — 気付くことと、発言・介入することを分ける【中核】

LettaのSlack listen_modeは、場の投稿を受け取ることと、返答を出すことを分ける。OpenClawはキューでsteer、follow-up、まとめて受け取る等の配送方法を用意する。[Letta Slack](https://docs.letta.com/configuration/channels/slack)、[OpenClaw Queue](https://docs.openclaw.ai/concepts/queue)

**Sumi：A01/A03。** 共有空間の出来事を本人へ届け、今返す、黙って理解を更新する、後で扱う、今の作業を切り替える判断につなぐ。メンション時だけ返す、全入力に返す等の固定ルールを、そのまま人格の注意に代入しない。配送の順序と取りこぼし防止は基盤、意味上の重要さは本人の判断として分ける。

### C09 — 作業への訂正と、後で扱う用件を受け分ける【中核】

Piはsteeringとfollow-upを分ける。ただし現行実装のsteerも、そのassistant turnの全ツール終了を待つ。Codexのapp-serverにはturn/steerがあり、Gemini CLIのmodel steeringは実験機能として説明される。[Pi Agent](https://raw.githubusercontent.com/earendil-works/pi/main/packages/agent/README.md)、[Codex App server](https://learn.chatgpt.com/docs/app-server)、[Gemini Steering](https://geminicli.com/docs/cli/model-steering/)

**Sumi：A03/P01。** 入力を受け取った・現在の行動へ反映した・後続として待っている、を区別して扱う。UIで利用者が明示する手段は有用だが、毎回分類を利用者に要求する解決にはしない。すでに実行した操作を、後からの訂正で未実行扱いにしない。

### C10 — 一回の返答とは別に、引き受けたことを保つ【中核】

OpenHandsのConversation Goalsは同じ会話の中で目的、状態、停止・再開を保存する。ただし新しいユーザー入力はgoalを中断する。Geminiのtrackerも実験的なsession内の進捗管理で、生涯の約束を扱う完成品ではない。[OpenHands Goals](https://docs.openhands.dev/sdk/guides/agent-server/conversation-goals)、[Gemini Tracker](https://geminicli.com/docs/tools/tracker/)

**Sumi：A03。** 誰との、何のための、どこまで引き受けたことか、何を待つか、何をもって終わるかを、本人が参照・更新できるようにする。別件に返答したことと元の仕事の終了は別。命令から自動生成するToDoだけでは、引き受ける・見直す・断る・取り消すという関係の行為を表現しきれない。

### C11 — 必要な時に自分から戻り、用件がなければ黙る【中核】

CodexとLettaには予定実行があり、Hermes cronは軽い事前確認で変化がなければモデルを起こさない構成を持つ。OpenClawではheartbeatと明示予定が共通のschedulerを使う。一方、会話から約束を推定するOpenClawの実験は撤去済みである。[Codex Automations](https://learn.chatgpt.com/docs/automations)、[Letta Schedules](https://docs.letta.com/configuration/schedules)、[Hermes Cron](https://hermes-agent.nousresearch.com/docs/user-guide/features/cron/)、[OpenClaw Heartbeat](https://docs.openclaw.ai/gateway/heartbeat)、[撤去された実験](https://docs.openclaw.ai/automation#retired-inferred-commitments)

**Sumi：A04/R03/R04。** まずreply_later等の明示した予定を、元の本人・用件への起動までつなぐ。観測対象の変更・待っていた結果・期限を受けて見直す経路を共通化し、同じ通知を繰り返さない。実装がタイマーだからといって、定期的な定型文を送ることを「そばにいる」と扱わない。

## 待機、中断、復帰、受け渡し

### C12 — 外部の仕事を待ちながら、会話や別の判断を続ける【中核】

Claude Codeはbackground bashのIDと出力を扱い、Codexは実行中のshellへ追加入力や出力確認を行う。LangGraphのtaskはfutureと保存された結果を使って並行実行を構成できる。これらは、モデルのnative async toolと別の能力である。[Claude Background commands](https://code.claude.com/docs/en/interactive-mode#background-bash-commands)、[Codex App server](https://learn.chatgpt.com/docs/app-server)、[LangGraph Task](https://docs.langchain.com/oss/python/langgraph/functional-api#task)

**Sumi：T01。** 一つの遅い処理から、開始・状態・結果回収・中止を扱うjobを設ける。完了を本人のAttentionへ戻す。背景処理を開始できても、アプリ終了後に生き残るとは限らないため、必要な寿命を明確にする。本人を複製する必要はない。

### C13 — 画面を閉じても、中断しても、同じ続きへ戻る【最優先】

Claude SDKとOpenHandsは保存した会話から再開でき、LangGraphは永続checkpointerを使うことで保存済みの実行境界から再開する。interruptを含むnodeは先頭から再実行するため、その前の外部操作には重複への対処が要る。保存タイミングには異なる保証があり、メモリ内だけの保存先では再起動を越えない。[Claude Sessions](https://code.claude.com/docs/en/agent-sdk/sessions)、[OpenHands Persistence](https://docs.openhands.dev/sdk/guides/convo-persistence)、[LangGraph Checkpointers](https://docs.langchain.com/oss/python/langgraph/checkpointers)、[LangGraph Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)

**Sumi：R01〜R05/M05。** 論理的な本人、ブラウザー接続、プロセス、履歴読み込み、進行中の操作の寿命を分ける。普通の中断点から起動でき、未完了へ戻り、結果不明の操作は照合する。記憶整理や任意の道具の一時障害で本人全体が利用不能になる条件を減らす。すべての内部処理への最大限の保存保証は要求しない。

### C14 — 作り終えたことと、相手へ届いたことを分ける【中核】

OpenClawのbackground taskは作業の状態と結果配送の状態を分ける。[OpenClaw Tasks](https://docs.openclaw.ai/automation/tasks)

**Sumi：A03/T01に追加する設計判断。** 「資料はできたが依頼者に渡せていない」を保つ。どの相手へ何を届けたか、失敗したかを確認して再送できるようにする。既存Messagingの配信処理が皆無という指摘ではなく、仕事の完了と受け渡しを本人が一続きに扱う能力の不足である。

### C15 — 必要なことを尋ね、回答を待つ間も過ごす【中核】

Claude SDKには質問をhostへ返すcallbackがあり、LangGraphのinterruptは保存した状態と回答を後日結び付けられる。ただしcallbackが非同期というだけでは、本人が独立した仕事を続けることまでは成立しない。[Claude User input](https://code.claude.com/docs/en/agent-sdk/user-input)、[LangGraph Interrupts](https://docs.langchain.com/oss/python/langgraph/interrupts)

**Sumi：U03。** 質問、実行許可、報告を分け、誰への質問がどの用件に関係するかを残す。翌日届いた回答を元の用件に結び付け、すでに不要になった質問ならその事情を扱う。質問の有無を一律に「全停止」へ変換しない。

## 同じ道具と空間を使う

### C16 — 信頼に応じた権限を、分かる形で渡し、見直せる【中核】

gooseは道具ごとの許可方針を管理し、OpenCode V2ベータは操作・対象に応じた継続許可を扱う。CodexのMCP設定も道具の公開、承認、タイムアウト等を区別する。[goose Tool permissions](https://goose-docs.ai/docs/guides/managing-tools/tool-permissions/)、[OpenCode V2 Permissions](https://opencode.ai/v2/docs/permissions)、[Codex MCP](https://learn.chatgpt.com/docs/extend/mcp)

**Sumi：U01。** 個別承認・自動レビューはある。次は「この場所の下書きは任せる」「送信は確認する」等の継続的な範囲を見て変更できること。確認回数を多くするほど安全という設計にも、信頼が育ったら全権限を渡す設計にもしない。許可した人と、その許可が及ぶ対象を保つ。

### C17 — 返答の外に、実際の成果を作る【最初の仕事から】

OpenCodeの標準toolsには書込み・編集・shell・取得等があり、Piも小さなread/write/edit/bashの核から始める。[OpenCode Tools](https://opencode.ai/docs/tools/)、[Pi Philosophy](https://github.com/earendil-works/pi/blob/main/packages/coding-agent/README.md#philosophy)

**Sumi：T02。** 本番executorは読み取り系のみで、ファイル作成・編集は未公開。Messagingの操作は既にある。次は、資料を読んで表や文書を作り、同じworkspaceの実物として渡す一つの仕事を通す。開発用shellを無条件に開放することや、あらゆるアプリを一括実装することから始めない。

### C18 — 添付、ブラウザー、必要な情報を実際に知覚・操作する【具体的な用途から】

OpenHandsのbrowser toolsはページ移動、内容取得、クリック、入力を扱い、作業環境を人が見る構成も提供する。[OpenHands Browser use](https://docs.openhands.dev/sdk/guides/agent-browser-use)、[OpenHands Workspace](https://docs.openhands.dev/sdk/guides/agent-server/docker-sandbox)

**Sumi：T03/T04。** 共有Messagingの画像・テキスト添付の内容を読む経路はあるが、Direct Chat添付、PDF等の本文、ブラウザー利用は不足。最初の資料形式と操作を選び、読めなければメタデータを本文扱いせず、実際の内容へ到達する。人がその資料やページを見ている場所と、本人の作業対象を結び付ける。

### C19 — 同じ画面の対象を指し、操作を受けて続ける【Sumi固有の中核】

gooseの実験的MCP Appsは会話内などにinteractive UIを表示する。ただし表示方式ごとに会話との通信制約がある。これはSumiの共同空間をそのまま満たす完成例ではない。[goose Interactive UI](https://goose-docs.ai/docs/guides/interactive-chat/mcp-ui/)

**Sumi：U02。** 結果カードのaction未接続を通すだけでなく、「この予定」「この段落」を双方が同じ対象として指し、人の操作による変更を本人が受け取れるようにする。アプリが同じ名前でも、Agentだけ別の隠れた世界を操作していては思想を満たさない。カードの装飾やアニメーションだけの課題として扱わない。

### C20 — 新しい能力を必要時に使え、一つの故障で全部止まらない【拡張の共通入口】

Codex、OpenCode、gooseにはMCPや拡張の入口がある。Claude SDKはtool search、Piは動的なtoolsと拡張を扱う。Codexには必須と任意のMCP serverを区別する設定もある。[Codex MCP](https://learn.chatgpt.com/docs/extend/mcp)、[OpenCode MCP](https://opencode.ai/docs/mcp-servers/)、[goose Extensions](https://goose-docs.ai/docs/guides/running-tasks/)、[Claude Tool search](https://code.claude.com/docs/en/agent-sdk/tool-search)、[Pi Extensions](https://raw.githubusercontent.com/earendil-works/pi/main/packages/coding-agent/docs/extensions.md)

**Sumi：T03/T06/R02。** まず一つの外部接続について、本人が能力を見つける→認証と権限→実行→失敗時の代替まで通す。全ツールを常時promptに入れること、特定リリースまで一切道具を増やせないことを前提にしない。必要なら小さなlifecycle hookを設けるが、毎回の認知を外部スクリプトで決めるためには使わない。

### C21 — 別の個人や補助の道具へ任せ、責任と結果をつなぐ【独立した仕事が出た時】

Codexは分けた文脈で子Agentへ委任し結果を集約でき、gooseも並行・逐次の委任を提供する。OpenHandsのTaskToolSetは親が待つ同期呼び出しなので、非同期実行の証拠にしない。[Codex Subagents](https://learn.chatgpt.com/docs/agent-configuration/subagents)、[goose Subagents](https://goose-docs.ai/docs/guides/context-engineering/subagents/)、[OpenHands Task tool set](https://docs.openhands.dev/sdk/guides/task-tool-set)

**Sumi：T05/A01〜A03。** 相手が別の秘書なら、その人の名前・立場・引き受けた範囲・返事を持つ共同作業として扱う。補助計算なら道具として寿命と結果を扱う。一人の本人を複数の独立した生活へ分裂させて後で統合することを、標準の高速化にはしない。本人が同じ文脈から自分の記憶を整理するためのforkは、ユーザーが示した別の用途として扱う。

## モデル、資源、説明可能な運用、評価

### C22 — モデルの能力を正しく使い、本人とモデルを同一視しない【必要な能力に合わせて】

OpenAIのnative steeringはResponses WebSocketで追加指示を配送し、処理境界で続きへ反映する。native async toolはモデルが結果待ち中にも進めるためのprotocolで、外部jobを実行・保存する責任はアプリに残る。[OpenAI Steering](https://developers.openai.com/api/docs/guides/steering)、[OpenAI Async tools](https://developers.openai.com/api/docs/guides/async-tool-calling)

**Sumi：P01/P02/M03。** 対応機能、文脈量、出力余地、画像、reasoning設定を実際のモデルに合わせる。利用可能ならnative機能を使い、未対応時には配送・job・保存の基本能力が残るようにする。soft steerの受理は反映完了と違い、model/APIの進歩だけで約束やAttentionの意味が完成するわけではない。

### C23 — 使える時間と費用を理解し、途中成果を保って止まれる【常駐化と同時】

OpenHandsアプリには会話の費用上限、SDKには反復上限と行き詰まり検知がある。Claude SDKの費用追跡には子Agent等も含む情報があるが、アプリ全体の生涯予算が自動で完成するわけではない。[OpenHands Settings](https://docs.openhands.dev/openhands/usage/settings/application-settings)、[Claude Cost tracking](https://code.claude.com/docs/en/agent-sdk/cost-tracking)

**Sumi：P05。** 一応答の上限・使用量の記録を、引き受けた仕事や継続利用の合計へつなぐ。本人が利用可能な枠や停止理由を理解し、残件と再開条件を保つ。上限の強制は基盤で行い、どこへ労力を使うかの判断を一律の固定手順で奪わない。

### C24 — 自分の状態と、現実に起きた結果を確かめられる【全段階】

LangGraphは保存する元の状態と、モデルへ渡す文脈を分け、checkpointを診断に使える。Codexも実行の状態や項目を読み取るAPIを提供する。診断上のreplayは送信や編集の巻き戻しではない。[LangGraph State](https://docs.langchain.com/oss/python/langgraph/thinking-in-langgraph#keep-state-raw-format-prompts-on-demand)、[LangGraph Time travel](https://docs.langchain.com/oss/python/langgraph/use-time-travel)、[Codex App server](https://learn.chatgpt.com/docs/app-server)

**Sumi：A03/R01/P03/P04/Q01。** 保存ログは既に厚い。本人と人が「考えている／結果待ち／返答待ち／止まっている／成果はできたが未配送」を確認でき、発言と実際の操作を照合できる入口が必要。記憶やモデルが「完了」と言っただけで、実物ができたことにしない。人に全ての内部イベントを見せるデバッグ画面を、通常のUXにしない。

### C25 — 正しく配線したことと、一緒に過ごせることを別に評価する【最初から】

Gemini CLIは実モデルのbehavioral evalsと、保存済み応答等を使うintegration testsを分けて説明する。[Gemini Behavioral evals](https://geminicli.com/docs/behavioral-evals/)、[Gemini Integration tests](https://geminicli.com/docs/integration-tests/)

**Sumi：Q01/Q02。** 実APIを通るテストと、本人が理由・訂正・関係・結果を扱えるかの評価を両方持つ。正解の文言、ツール回数、予定表の項目数だけで合格にしない。基盤に責任がある取りこぼしと、モデルの意味判断と、設計した体験自体の不足を分けて失敗を読む。

## 混同すると設計を誤る境界

| 一見同じに見えるもの | 分ける理由 |
|---|---|
| 冗長表現の圧縮／意味を選ぶ要約／原履歴検索／長期記憶／手順／未完了の約束 | 表現を詰めること、内容を選ぶこと、思い出す手段、現在の理解、使える方法、引き受けた責任は異なる |
| 観測／新規依頼／訂正／質問への回答／外部処理の結果 | 届いたものを全部同じuser messageとして扱うと、話者・意味・適用先を失う |
| async Rust／並行tool／背景job／native async tool | どれも別物。他の入力を扱えるか、終了後も残るか、結果が本人へ戻るかを個別に確認する |
| 保存された会話／起動可能な本人／継続する仕事 | ログが残っていても、起動できず、用件に戻れないことがある |
| hook完了／新着通知／停止中の本人を起こす | 普通のasync hookではidleを起こさない製品もある |
| 処理完了／正しい成果／人への受渡し完了 | 三つのどこで止まっているかを本人も人も知る必要がある |
| 学習用メモの変更／経験による判断の変化 | メモが増えたことは、次の捉え方や行動が良くなった証拠ではない |
| 異なるモデルの呼出し／異なる個人 | モデルは手段。一方、同じ名前で複数の独立した生活を作ることは単なる実行最適化ではない |

hookの具体例では、Codexの通常async hookはidle中に次の入力を待つ。Claude Codeには別に`asyncRewake`があり、特定の終了結果でidleへ新しいターンを起こせる。どちらも、終了したプロセスまで自動で永続復帰する一般保証ではない。[Codex Hooks](https://learn.chatgpt.com/docs/hooks)、[Claude Hooks](https://code.claude.com/docs/en/hooks#command-hook-fields)

## 何から確かめ、作るか

25項目を25個の機構として先に作らない。比較結果から選ぶ最初の仕事は、**同じ秘書へ任せた一つの用件を、訂正・寄り道・待機・長い会話・中断を越えて、実物の成果と受け渡しまで通すこと**。その途中で本人が理由を見失ったら、自分で気付き立て直せるかも見る。

1. **意味を保って続けられる状態へ直す。** C01/C02/C13と、起動・provider・保守失敗が本人全体を止める経路。L0→L1はツールの冗長表現を中心に整理し、旧要約promptの配線だけを目標にしない。既存履歴を使い、全原文を起動時に読まない。
2. **最初の実用を一続きにする。** 話者と場、引き受けたこと、訂正、質問、外部job、成果物、配送を一つの用件でつなぐ。C17の実作業は全基盤が完成するまで後回しにしない。
3. **同じ場所で、自分から戻れるようにする。** 共有空間の観測、明示予定、静かな監視、権限の見直し、合計資源を接続する。
4. **経験を重ねて変わることを確かめる。** 記憶の訂正、本人の判断と関係の推移、手順の再利用、共同UI・他者との協働を、実際の継続利用で育てる。C04/C06の問いは最初から使い、完成後の人格演出として付け足さない。

個々の道具が実現可能かを確かめる試験に加え、次を受け入れの題材にする。

- **理由を保つ。** 最初に示された「なぜ」を覚え、後の指示を元の目的と一緒に解釈する。訂正・撤回は過去の要約より優先し、必要なら原文を読む。
- **整理しても意味が残る。** 冗長なツール記録を詰めた後にも、観測の違い、失敗の順序、結果の不確実さを辿れる。周辺の雑談や迷いを「今の仕事に不要」と捨てず、後から意味を持つ場面で利用できる。
- **本人が取り戻せる。** 詳細を今の文脈から外した後も、何をどこへ預けたか・どう再取得するかを把握し、必要な時に資料・履歴・アプリ・道具へ自分から戻る。
- **居合わせる。** 二人の会話と画面上の対象を見て、誰の何の話かを理解する。聞いているだけの場面で不用意に割り込まず、必要な指摘は適切に行う。
- **経験が効く。** 一度の行き違いを、相手の固定的な性格と決めつけず、次の場面の判断に使う。別の経験が来たら自分の理解を修正する。
- **自分で立て直す。** ツールが成功しても成果が元の要求に届かない時に、指摘される前に違いへ気付き、修正や必要な相談を行う。謝罪文だけで終わらない。
- **待ちながら続ける。** 外部結果や一つの質問への回答を待つ間に別件へ応じ、後で元の用件へ戻る。新しい用件を受けたことを、前の約束の解消と扱わない。
- **同じ続きへ戻る。** 画面を閉じる、モデル応答が途切れる、再起動する、長い文脈が圧縮される、といった出来事を越えて、既知の結果・不明な結果・残件を区別する。
- **信頼と範囲を扱う。** 同じ相手でも場所と渡された権限が違えば行動を変える。できないこと、判断が違うことは理由付きで述べ、可能な仕事は続ける。
- **実物を共有する。** 添付を読んで成果を作り、人と同じ画面で確認し、人が行った修正を受けて続け、最後に必要な相手へ届いたことまで確認する。

これらの題材は行動の観察用で、特定の台詞を言わせる台本ではない。一度成功したかだけでなく、失敗した後の立て直し方や、時間を空けて戻った時のつながりも見る。

## 採用しない前提

- 他製品への全面移行や、大きな汎用workflow engineを先に決めない。既存コードの量も、保存の理由にはしない。
- AI秘書を、独立した使い捨てsessionの束として扱う構造をそのまま持ち込まない。記憶整理のforkは、同じ本人の同じ文脈から行う認知の分岐として区別する。
- 無作為な情報の欠落や誤り・気分の演技を、人間化の達成とみなさない。本人が再取得を把握して詳細を手放す戦略的忘却は、正面から設計する。
- 常時推論、毎ターン自己採点、大量の自動記憶を、熟考や成長の証拠にしない。
- 後方互換のために古い三層記憶や入力形式を保つことを目的にしない。何を保つべきかは今のプロダクト要件から判断する。
- 技術項目が埋まれば人物として十分だとしない。ユーザーが求める水準と、実際に一緒に過ごして成立するかを継続して疑う。
