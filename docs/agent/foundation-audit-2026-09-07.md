# Sumi エージェント基盤の実装監査 — 2026-09-07

## 結論と調査の範囲

現状は、保存された会話に対して応答し、限られたアプリ操作を行うAgentとしての部品はある。一方、長く一緒に過ごす秘書に必要な「覚える・気付く・任された仕事を保つ・待つ間も動く・障害後に戻る」が本番の一続きの動作になっていない。これはコード量やテスト数からは判断できない。

さらに、仕事を完遂するだけではREADMEとユーザーの求める一個人には届かない。同じ場での経験や本人の理解・関係の変化を、私の設計が支えているかを問う。ユーザーの「迷走を自ら是正できるメタ認知」は、その考察を続ける私（Codex）への要求であり、Sumi自身の必須機能とは扱わない。本書は実装上の根拠を整理し、人物としての設計仮説と外部10製品・1基盤からの採用判断は [比較レポート](foundation-comparison-2026-09-07.md) で扱う。

調査対象は `9675b3cf`。監査対象の `apps/agent`、`apps/api`、`.github`、`docs/agent` は、調査時のGitHub main `5918a69f252c83da4fdc9cf677f710243626c75d` と差分がない。6分野を並行して読み、bootstrap、呼び出し元、実際に組み立てる入力、保存・復旧経路、代表的なテストを照合した。

- **確認済み**はコード上の動作・配線を指す。以下の利用例は、その動作から考えられる影響であり、今回障害を再現したという意味ではない。
- **未接続**は部品が存在しても本番から使われないもの。**未実装**は対象の本番経路に能力がないもの。**部分実装**は既存能力の限界。**要修正**は既存経路の動作上の問題。
- 今回は調査。テスト・ビルド・実モデル呼び出し・デプロイ・依存追加・Docker操作は行っていない。既存の原文保存や権限制御を削除していない。
- 優先度は **先行**＝現在の利用・継続を壊す問題、**中核**＝秘書として成立させる能力、**拡張**＝先行・中核に続いて段階的に整える能力。セキュリティ脆弱性の重大度ではない。

既存ADRやIssueの「正典」「凍結」「承認必須」は、今回の採用判断の根拠にしていない。既存の圧縮promptもユーザーの思想ではなく、実装AIの仮置きであると今回確認した。ユーザーが示したL0→L1の方向は、主にツール呼び出しの冗長な表現を圧縮し、情報の質的な意味の劣化を可能な限り防ぐこと。親をforkして同じコンテキストの本人が記憶を扱い、詳細を外しても資料・履歴・道具等から取り戻せると本人が把握する戦略的忘却も扱う。この方針に照らした採用判断は比較レポートと [記憶の設計検討](memory-experience-design-2026-09-07.md) に記載した。

## すでに役立つ基盤

- 人格IDとプロセス世代を分けた実行・認可、永続的な会話とイベントの保存。
- Chat Completions / Responses / Anthropic の実アダプター、ストリーミング、基本的な再試行、使用トークンの記録。
- 接続の再確立、履歴再送、保存済み未分類入力の再開、重複入力の抑制。
- ツールの実行権限確認、実行済みか不明な外部操作を無条件に二重実行しない処理。
- Messagingの検索・読み書き・スレッド・投票・返信予定などの実ツール。画像とテキスト添付の内容を取り出す経路。
- ツール呼び出しと結果を一緒に保つ文脈処理、原文とモデル固有の非公開コンテキストの分離。

これらは残す価値がある。以下は「全部ない」という監査ではなく、これらを使って何が成立していないかの監査である。

## 記憶・Compact

### M01 — Compactの生成・適用が本番で一周しない【未接続／先行】

3本のプロンプト、圧縮ジョブ、worker、要約保存は存在する。しかし `CompactWorker::spawn` の本番呼び出しがなく、完了をSessionへ通知する `maintenance_ready` もテスト以外から呼ばれない。L1/L2の容量による再圧縮選択も未接続。provider-native Compactも本番の既定オプションでは無効で、Responsesの専用処理は呼ばれない。

**利用への影響：** 長い会話をしても、設計上のL1/L2記憶が通常経路で生成されない。「プロンプトを書けば完成」「workerを起動すれば完成」のどちらでもない。旧workerと仮置きpromptをそのまま接続することを、必要な修理だと決めてはいない。

根拠：[prompt loader](../../apps/agent/src/prompts.rs)、[worker](../../apps/agent/src/memory/compactor.rs) L2154、[bootstrap](../../apps/agent/src/bootstrap.rs) L1261、[Session](../../apps/agent/src/agent/mod.rs) L1264、[native処理](../../apps/agent/src/provider/mod.rs) L238。

### M02 — Compactの保持方針と、成功とみなす条件が弱い【部分実装・潜在不具合／先行】

3本は各16行で、出来事・ユーザー情報・約束・参照という同じ骨子と約800トークン上限を持つ。訂正・撤回・未完了と完了・事実と推測・実行済みか不明な操作の扱いが薄い。「ユーザーについて分かったこと」が中心で、本人の経験による自己理解や関係の捉え方の変化、判断を改めた理由を明示的には残さない。自由文に残せないという意味ではなく、その意味を守る指示と評価がない。応答解析には、生成打ち切りや一部プロトコルの空文字を完全な要約と区別しない経路がある。本番生成が未接続のため、これは稼働中に観測した誤要約ではない。

追加確認では、要約前の入力整形も通常ToolCallのIDと実行経路を落とし、時刻を秒精度へ変え、画像を一律に省略する。この入力を受けた後でpromptだけを改善しても、落ちた内容は復元できない。既存経路をそのまま起動する前に扱うべき潜在的な情報欠落である。

さらに、親のPromptContextを共有せず、専用system/instructionsと対象1バッチを文字列化したuser messageを独自に作り、toolsとprovider contextも引き継がない。既定で同じモデルを選んでも、親の長い送信履歴と共通のprefixにはならない。親と同じ文脈の本人が記憶を扱うという要件に合わず、親の履歴キャッシュの再利用も妨げる構造である。実ヒット率や課金額は計測していない。

**利用への影響：** 「以前そう言ったが、後で取り消した」の後半や、依頼した理由が圧縮で失われ得る。骨子の文字列が入っているテストでは判断できない。

根拠：[L0→L1](../../apps/agent/prompts/compact-l0-to-l1.md)、[監査時のL1→L2](https://github.com/sumi-studio/sumi/blob/2c674e26bfb9cf1f547fc9129389c0212d32aa5f/apps/agent/prompts/compact-l1-to-l2.md)、[監査時のL2統合](https://github.com/sumi-studio/sumi/blob/2c674e26bfb9cf1f547fc9129389c0212d32aa5f/apps/agent/prompts/compact-l2-consolidation.md)、[compactor](../../apps/agent/src/memory/compactor.rs) L230・360・543・668・769・787・800・1062、[親の文脈](../../apps/agent/src/memory/context_assembler.rs) L326・355。

### M03 — 現在動く長文対策は、要約を待たず古い文脈を外す【要修正／先行】

L0が40k〜48kを超えると、古い会話単位から30kまで減らす。最新入力やツールの組は守るが、元の目的・理由・制約・未完了の約束は選別して保護しない。system、tools、記憶を含む全体の予算とモデルの窓を基準にした削減でもない。

**利用への影響：** 最初の依頼と制約がモデルの入力から外れ、最後の「続けて」だけが残り得る。原文のディスク削除ではないが、モデルが使える記憶は失われる。大きい窓を早く捨て、小さい窓では削減不足になる可能性もある。

根拠：[overflow](../../apps/agent/src/memory/overflow.rs) L151・188、[context assembler](../../apps/agent/src/memory/context_assembler.rs) L326・351・683、[固定値](../../apps/agent/src/memory/mod.rs) L35。

### M04 — 私的な過去の会話を探し直す・記憶を訂正する道具がない【未実装／中核】

原文は保存されるが、本番ツールに人格Agentの私的な履歴の検索・記憶の追加訂正・忘却管理がない。Messaging検索は共有空間のメッセージが対象であり、この履歴検索とは別。原履歴にはID・順序・時刻・会話ロールがあり、要約には元バッチへの参照もあるが、本人からその出来事や当時表明した判断理由へ戻る入口につながっていない。

**利用への影響：** 「先月その案を却下した理由」を文脈から落ちた後に調べ直せない。モデルが自分用のノートを作るためのファイル書き込みも本番にはない。

根拠：[登録ツール](../../apps/agent/src/bootstrap.rs) L1171、[executor登録](../../apps/agent/src/tools/executor/remote.rs) L171、[記憶投入](../../apps/agent/src/memory/context_assembler.rs) L1008、[原履歴と検索関数](../../apps/agent/src/store/transcript.rs) L26・127。

### M05 — 記憶の保守失敗から会話停止へ波及する【部分実装／先行】

未接続の要約経路には、失敗した要約が後続の適用を塞ぐ潜在問題があり、`reclaim_failed` の本番呼び出しはない。一方、現行Sessionには、特定のDeferredApply状態でIdle時に記憶の遷移が起きないと失敗する経路がある。通常の40k削減だけで必ず停止する、という指摘ではない。

**利用への影響：** 原文が正常でも、記憶の整理待ちが会話の利用不能になる。保守の遅れと、利用を止める必要のある異常を区別した回復が必要。

根拠：[compactor](../../apps/agent/src/memory/compactor.rs) L1794・2193、[Session](../../apps/agent/src/agent/mod.rs) L2266、[該当テスト](../../apps/agent/src/agent/session_tests.rs) L6424。

## Attention・仕事の継続

### A01 — 共有空間の出来事が、本人の注意へ届かない【未接続／中核】

共有Messagingの通知・未読状態は存在するが、Agentの実行を起こして本人に知らせる入口は未接続。実行側の入力はDirect Chat由来に固定され、Agentが自分でoverview/openする経路とはつながっていない。

**利用への影響：** 同じ場で名前を呼ばれても、直接チャットされるか自分で確認しない限り気付けない。

根拠：[gateway入力](../../apps/agent/src/gateway/mod.rs) L53・120、[由来の型](../../apps/agent/src/runtime/contracts.rs) L119、[通知配送](../../apps/api/internal/messaging/http.go) L575、[overview](../../apps/api/internal/messaging/overview.go)。

### A02 — 誰が・どこで・いつ、という意味上の情報がモデルに足りない【部分実装／中核】

認証・認可側は話者を保持するが、モデルへのUserMessageは本文と時刻へ縮小され、Chat Completionsでは時刻も出力されない。既定のsystem promptに現在時刻・タイムゾーン・個人プロフィールを供給する組み立てがない。認可を突破できるという指摘ではない。

**利用への影響：** 複数人の「私の予定」を誰のものか区別する材料や、日付を跨いだ「今日」を解釈する材料が不足する。

根拠：[入力構築](../../apps/agent/src/agent/run.rs) L3369、[steer](../../apps/agent/src/agent/steer.rs) L757、[送信](../../apps/agent/src/provider/adapters/chat_completions.rs) L311、[context](../../apps/agent/src/memory/context_assembler.rs) L348。

### A03 — 任された仕事が、個々の応答とは別に残らない【未実装／中核】

原依頼は会話に残るが、未完了・保留理由・再開条件・完了条件を継続して扱う仕事の記録がない。追加入力の処理は実行フェーズ依存。対応済みの復旧経路も `AgentEnd` と `CommandApplied` で閉じ、未完了の依頼を自発的に再評価しない。

**利用への影響：** 途中で進捗を聞く、別件を挟む、再起動するだけで、元の仕事へ戻る責任が曖昧になる。「履歴がある」と「仕事を続ける」は別。

根拠：[steer分類](../../apps/agent/src/agent/steer.rs) L48、[復旧](../../apps/agent/src/store/recovery.rs) L842・878・904、[実運用prompt](../../apps/agent/prompts/system.md) L3。

### A04 — 予定・返信の約束が、時刻到来時の行動へつながらない【部分実装／中核】

`reply_later` と `remind_at` は保存できるが、Agent起床はTODO。定期確認・結果待ち・何もなければ黙る監視も、人格の継続経路として成立していない。

**利用への影響：** 「30分後に返事する」を記録しても、30分後に本人が起きて対応しない。最初は明示的な予定から動かし、会話から勝手に約束を推測して増やすかは別の判断にする。

根拠：[返信予定ツール](../../apps/agent/src/tools/messaging.rs) L2352、[予定保存と未接続起床](../../apps/api/internal/messaging/local_control.go) L870・888。

## 道具・待機・能力の拡張

### T01 — 外部作業を待つ間に、本人が別件を考えられない【部分実装／中核】

ツールは逐次実行し、現在の実行が終わるまで次のモデル呼び出しへ進まない。進捗表示・キャンセル・steer受領はあるが、永続的なジョブの開始・状態取得・結果回収・再接続という口はない。

**利用への影響：** 長い処理の間、短い質問にもすぐ対応できない。人格を複製せず、作業の寿命を応答の寿命から分ける必要がある。

根拠：[実行ループ](../../apps/agent/src/agent/run.rs) L1525・2157・3040、[executor protocol](../../apps/agent/src/tools/executor/protocol.rs) L127、[process内結果管理](../../apps/agent/src/tools/executor/manager.rs) L20・49。

### T02 — 本番ではファイルを作成・編集して渡せない【未接続／中核】

本番executorはread_file/list_dir/glob/grepのみ。write/edit/delete/bashは開発・テスト側にあり、本番サービスも除外している。Messagingで既存ファイルを送ることはできる。

**利用への影響：** 「この内容でCSVや資料を作って送って」が成立しない。開発用bashを登録するだけで済ませず、まず限定した成果物の作成・編集・受け渡しを通す。

根拠：[本番登録](../../apps/agent/src/tools/executor/remote.rs) L171・2080、[service制限](../../apps/agent/src/tools/executor/service.rs) L1419。

### T03 — 外部サービス・検索・ブラウザを接続する実用経路がない【未実装／中核】

本番は組み込みのSumiツールとファイル参照に限られる。Rustアダプター境界はあるが、MCPクライアント、ブラウザ操作、外部接続の導入・認証・発見経路はない。

**利用への影響：** URLの内容を確かめる、既存サービスに接続するだけでも新しい組み込み実装が必要。全アプリの完成を待つ前に、ひとつの外部サービスを安全に接続できる基盤が要る。

根拠：[登録全体](../../apps/agent/src/bootstrap.rs) L1171、[adapter境界](../../apps/agent/src/tools/mod.rs) L279・293。

### T04 — 添付の入口と、読める形式が不十分【部分実装／中核】

Direct Chatは添付欄を非表示にし、gatewayも空の添付配列しか受け付けない。共有Messaging側は画像・UTF-8テキスト・JSONを取り出せるが、PDF・Office・音声・動画は内容でなくメタデータを成功結果として返す。画像を渡せても、選択モデルの画像能力には依存する。

**利用への影響：** 同じ「添付を開く」でも、画像を見る場合とファイル名だけ読む場合がある。非対応状態の明示、参照可能なファイル、必要なページだけ読む処理が必要。

根拠：[Direct Chat UI](../../apps/web/src/components/chat-prompt-input.tsx) L31、[gateway](../../apps/agent/src/gateway/mod.rs) L106、[添付内容](../../apps/agent/src/tools/messaging.rs) L2691。

### T05 — 専門作業を補助Agentへ任せて回収する道具がない【未実装／拡張】

本番ツールに補助Agentの開始・進捗・追加連絡・取消・結果回収がない。並列調査で大きなログを本人の文脈に入れず、成果だけ戻すことができない。

**利用への影響：** 複数資料の調査などを一人の逐次処理へ詰め込む。導入する場合も、秘書本人の独立した人格を何個も作る設計とは分ける。

根拠：[本番ツールの全構成](../../apps/agent/src/bootstrap.rs) L1171、[Tool境界](../../apps/agent/src/tools/mod.rs) L279。

### T06 — 学んだ手順や拡張を必要時に読み込む経路がない【未実装／拡張】

本番のツール集合は起動時に固定され、技能の一覧・選択・必要時の詳しい読み込み・導入更新という実行経路がない。ファイルを読めること自体は、技能を発見・活用する機能とは異なる。

**利用への影響：** 毎回同じ手順を説明するか、コアのプロンプト・コードへ知識を詰め込むことになる。必要なのは再利用できる手順であり、スキルの指示を本人の判断より強くすることではない。

根拠：[固定定義投入](../../apps/agent/src/bootstrap.rs) L1232、[ツール集合](../../apps/agent/src/tools/mod.rs) L689、[system prompt](../../apps/agent/prompts/system.md)。

## 障害・長期稼働

### R01 — 普通の中断地点にも、起動できない復旧状態が残る【要修正／先行】

復旧計画は多くの段階を認識するが、本番の実行側は限定した1段階の組み合わせしか処理せず、複数段階や未対応フェーズで失敗する。生成途中、入力確定から生成開始まで、steer適用途中などが対象になり得る。未分類の受信済み入力を戻す経路は別途実装済み。

根拠：[起動時復旧](../../apps/agent/src/bootstrap.rs) L439、[実行可能な復旧計画](../../apps/agent/src/store/recovery.rs) L228・237・265、[計画生成](../../apps/agent/src/store/recovery.rs) L1544・1602・1613。

### R02 — 道具の一度の不調で、会話する本人まで終了する【要修正／先行】

executorの2秒ヘルスチェックが最初に失敗すると、依存監視が終了を通知し、Sessionごと終了する。一時的な接続・I/O遅延と、認証やプロセス同一性の破綻が同じ終了経路へ流れる。

**利用への影響：** 単なるテキスト回答中でも道具側の一時不調で停止し、R01の復旧問題を引き起こし得る。影響する道具の利用停止と人格全体の終了を分ける。

根拠：[ヘルスチェック](../../apps/agent/src/bootstrap.rs) L474・581、[全体終了](../../apps/agent/src/bootstrap.rs) L2031・2075。

### R03 — 画面を閉じると、仕事中でも休止対象になり得る【要修正／先行】

Coldの既定休止は5分。判定は最終ブラウザ活動に依存し、実行中の仕事を保護する判定がない。一方、放置した画面のpongは活動時刻を更新する。実デプロイが休止時間を上書きしている可能性はある。

**利用への影響：** 任せてノートPCを閉じると、長い処理が終わる前に止まり得る。ブラウザの存在と仕事の存続を切り離す必要がある。

根拠：[既定時間](../../apps/api/cmd/server/main.go) L2322、[休止判定](../../apps/api/internal/spawn/manager.go) L341、[活動更新](../../apps/api/internal/agentevents/browser_ws.go) L756・939・1011。

### R04 — Warmは「止めない」だけで、止まった後に戻さない【部分実装／中核】

WarmはIdle停止から外す条件。終了後の再開やAPI再起動後の復元を保証せず、次の直接会話が起動契機になる。Managerは終了理由を捨てる。ログ自体が存在しないという意味ではない。

根拠：[Manager](../../apps/api/internal/spawn/manager.go) L146・275・349、[起動時処理](../../apps/api/cmd/server/main.go) L98。

### R05 — 履歴が増えると、ある日再起動できなくなる固定上限【要修正／先行】

起動時に生涯の会話・provider context・記憶・ジョブ等を合計し、10万行または64MiBを超えると拒否する。必要な作業文脈だけの上限ではない。全履歴をメモリに展開する構造なので、モデル向けCompactを直すだけでは解消しない。

**利用への影響：** 長く使った人格ほど次の再起動で戻れなくなる。直近の作業状態だけで起動し、過去を必要時に読む構造が必要。

根拠：[Store](../../apps/agent/src/store/mod.rs) L158・179・702・1535・1661。

## モデル・費用・回復の制御

### P01 — モデルのnative steering／async toolを使う口がない【未実装／拡張】

全providerがHTTP POST＋SSEで、Responses WebSocketのsteerを受け付ける接続・状態がない。ツール定義も同期function形式に限られ、保留中の結果を後から戻すモデル側の契約がない。Rustでasyncを使うこととは別。

根拠：[Responses通信](../../apps/agent/src/provider/mod.rs) L1102、[ツール型](../../apps/agent/src/provider/types.rs) L1209、[wire生成](../../apps/agent/src/provider/adapters/responses.rs) L423。

### P02 — モデル選択・能力設定・思考量が十分に制御できない【部分実装／中核】

起動時プリセットの能力・制限がモデルID変更後も引き継がれる。会話中のモデル変更経路はなく、reasoning_effortは本番設定から渡せない。Anthropicのthinking budgetは1024固定。実際の回答品質への影響は評価が必要。

根拠：[config](../../apps/agent/src/config.rs) L53・274・291、[本番options](../../apps/agent/src/bootstrap.rs) L1263、[Anthropic](../../apps/agent/src/provider/adapters/anthropic.rs) L143。

### P03 — 無害なprovider側の追加情報まで拒否する【要修正／先行】

既知のreasoningオブジェクトに未知の付加プロパティがあると、使える内容でも `invalid_provider_stream` にして終了する。未知のイベント種別全体を拒否するわけではない。追加プロパティを拒否するテストがこの挙動を固定している。

根拠：[検証](../../apps/agent/src/provider/adapters/responses.rs) L3484・3605、[該当テスト](../../apps/agent/src/provider/adapters/responses.rs) L5881、[終了](../../apps/agent/src/provider/mod.rs) L1202・2511。

### P04 — 待てば戻る混雑に、指定された待ち時間で対応しない【部分実装／先行】

Retry-After等のヘッダーを捨て、2・4・8秒の固定再試行で終了する。「1分待つ必要がある」場合、待機時間合計14秒で全試行を使い切り得る。基本再試行・エラー分類・中断は実装済み。

根拠：[transport](../../apps/agent/src/provider/transport.rs) L21・64、[retry](../../apps/agent/src/provider/retry.rs) L14・114。

### P05 — 1応答の上限はあるが、任せた仕事全体の予算がない【部分実装／中核】

1応答のトークン・通信量・イベント数、ネットワーク待ち、特定の失敗ループは制限し、利用量も保存する。しかし成功したツール→推論の繰り返しに、総ステップ・総時間・総トークン・金額の事前確認がない。日・人格・雇用者単位の予算も、この実行経路には接続されていない。外部provider自身の課金上限は調査していない。

**利用への影響：** 各応答は制限内でも合計消費が増え続ける。自律起床や並列化と一緒に、止まる条件・相談・再開可能な残件の記録が必要。

根拠：[使用量型](../../apps/agent/src/provider/types.rs) L539、[応答予算](../../apps/agent/src/provider/assembler.rs) L29、[継続ループ](../../apps/agent/src/agent/run.rs) L631・758・858・958。

## 人との接点・使える権限

### U01 — 「今後これを任せる」を管理可能な許可へ変えられない【部分実装／中核】

本番ポリシーは固定baseline。Read以外は毎回AutoReviewへ流れ、ユーザーのUIは今回のみ許可／拒否に限られる。継続許可の部品はあっても、導入・確認・縮小・撤回という本番導線がない。全操作で人間に質問するという意味ではない。

根拠：[baseline](../../apps/agent/src/bootstrap.rs) L1192、[route policy](../../apps/agent/src/approval/route_policy.rs) L201、[review](../../apps/agent/src/approval/route_broker.rs) L488、[UI](../../apps/web/src/components/approval-confirmation.tsx) L20・60。

### U02 — 動的UIを見せる部品と、共同操作の往復が分断されている【未接続／拡張】

SDUIの結果をカード化する受け口はあるが、ChatItemはonActionを渡さず、カードのボタンは無効になる。MCP Appのフレームも認証されたresource配送待ちのdormant表示。共有の意味・空間を認識し操作する汎用経路が完成している証拠にはならない。

根拠：[結果のカード化](../../apps/web/src/agent/reducer.ts) L499、[表示](../../apps/web/src/components/chat-item.tsx) L97、[操作無効化](../../packages/sdui/src/renderer.tsx) L95、[MCP App](../../apps/web/src/components/mcp-app-frame.tsx) L55。

### U03 — 質問を出して、回答不要の仕事を続ける仕組みがない【未実装／中核】

自然文で質問することと、一件の操作承認を待つことはできる。しかし本番ツールとgatewayには、質問ID・回答待ち・回答の到着を仕事へ結び付けつつ独立作業を進める口がない。

**利用への影響：** 一点だけ確認したいときに全作業が止まるか、会話の後続に埋もれる。質問・承認・単なる報告を区別し、同じ未完了の仕事へ返す導線が必要。

根拠：[入力種類](../../apps/agent/src/gateway/mod.rs) L53、[本番ツール](../../apps/agent/src/bootstrap.rs) L1171、[承認待機](../../apps/agent/src/agent/run.rs) L2055。

## 評価と実装の複雑さ

### Q01 — 実モデルが秘書として完遂するかを測れていない【不足／先行】

real-agent E2Eは本物のAPI・runtime・executor・ブラウザを通す有用な試験だが、モデル役は固定順序のツールを返すLoopbackChatProvider。訂正を覚える、誰の依頼か理解する、適切に相談する、実際に仕事を終える品質を示さない。今回確認した範囲には、その行動評価の反復可能なセットがない。過去の実モデル1往復の確認も、その代わりではない。

標準system promptには、間違いを認めて修正する、知識を過信しない、自分の未実装能力を捏造しない指示は既にある。モデルが現在の文脈内で省察できないという指摘でもない。M04の不足は、本人が必要と思った時に履歴や記録を扱える手段の不足である。反省→訂正の保存→次回の改善という循環を内部に固定することは、この指摘に含めない。仕事の品質は実際の要求と成果で確認し、自己改善の循環があることを人物性の判定に使わない（比較レポートC06）。

根拠：[固定モデル役](../../apps/web/e2e/support/real-agent-stack.ts) L170、[回数確認](../../apps/web/e2e/real-agent-chat.spec.ts) L274・539、[prompt文字列テスト](../../apps/agent/src/prompts.rs) L36、[既存の行動指示](../../apps/agent/prompts/system.md) L72・77・95。

### Q02 — テストの量・内容・実際の実行が噛み合っていない【不足／先行】

リポジトリに宣言されたJenkinsはDocker build/push、DockerfileはRust buildを行うが、Rust回帰テストの実行経路がない。GitHub workflowにも該当jobはない。外部Jenkins設定やbranch protectionは未調査。一部のテストはpromptの言い回し、Composeの綴り、過去のrunbookのコードブロック数を固定している。

**判断：** テストを一括削除しない。実行中ツールを壊さないsteerや、実SIGKILL＋SQLite復旧＋重複入力のテストは有用。必要な既存試験を実行し、ユーザー要件を守らない古い固定だけを整理する。

根拠：[Jenkins](../../Jenkinsfile) L147、[Dockerfile](../../deploy/agent/Dockerfile) L8、[静的試験](../../scripts/dev/real-stack-static.test.mjs) L82、[有用なsteer試験](../../apps/agent/src/agent/run/tests.rs) L5392、[有用な復旧試験](../../apps/agent/src/agent/session_tests.rs) L12040。

## 改善の進め方についての判断

最初から30件を独立PRに分けたり、巨大な新アーキテクチャを設計したりしない。利用者が任せる一つの仕事を通して、必要な接続と単純化を進める。

1. **長く過ごしても利用不能にならず、経験の意味を保つ。** M01〜M05、R01〜R05、P03/P04のうち実行を止める経路を優先する。L0→L1では主にツールの冗長表現を整理する。原文保存、モデルの文脈、長期記憶、起動時の読み込みを分け、当面の仕事に不要という理由だけで経験を要点へ縮めない。
2. **一つの仕事を引き受け、確認や中断を挟んでも実物を渡す。** A02/A03、T01/T02、U03。最小の未完了記録、質問、外部結果、復旧を、必要な資料作成と受け渡しまで一周させる。実作業を全基盤の完成待ちにしない。
3. **同じ場所で気付き、適切な時に動く。** A01/A04、U01、P05。話者と場所、明示的な予定、繰り返さない通知、許可と予算を合わせて通す。
4. **必要になった能力と共同操作を広げる。** T03〜T06、U02。最初の実用に続く具体的な場面から、添付、外部検索、拡張、他者との協働、共同UIをつなぐ。モデル固有の機能は必要な箇所でP01/P02として使う。

## 行動で確認する最小の題材

以下は監査から私が提案する評価の題材であり、各項目をユーザーが直接指定したという意味ではない。

- 最初の依頼と理由、後からの訂正を含む長い会話を圧縮し、目的・撤回・残件を保って続ける。
- 過去の判断の根拠を探し直し、原文への参照とともに説明する。
- 仕事中に短い別件と確認への返答を挟み、元の仕事を終える。
- 外部処理中に画面を閉じ、再接続して同じ仕事と結果を受け取る。
- いくつかの実際の中断地点から戻り、二重実行せず、未確認の結果は確認する。
- 同じAgentへ複数人が話し、話者・場所・許可を取り違えない。
- 指定時刻に起きて確認し、変化がないときは何も送らず、必要なときだけ連絡する。
- 添付資料を読んでファイルを作り、許可された相手へ渡す。
- 混雑と予算到達時に依頼を失わず、理由・残件・再開方法を示す。
- 以前案を却下した経緯を本人が読み返したい時に、教訓だけでなく、当時の言葉や理由へ戻れる。
- 同じ資料を複数人で見ながら話し、人が変えた対象と会話を理解して、参加する・聞いている・今は介入しない判断をする。
- 自分が観測できなかった期間を、ずっと見ていたように語らず、必要な出来事を調べて関わり直す。

## 既存Issueとの対応（未更新）

| 話題 | 既存Issue | 扱い |
|---|---|---|
| Compact | [#63](https://github.com/sumi-studio/sumi/issues/63)、[#57](https://github.com/sumi-studio/sumi/issues/57) | prompt外部化は部分的に済んでいる。まず本番の生成→適用へscopeを更新する。古い三層構造を必須としない。 |
| Attention・未完了の仕事 | [#87](https://github.com/sumi-studio/sumi/issues/87)、[#173](https://github.com/sumi-studio/sumi/issues/173)、[#172](https://github.com/sumi-studio/sumi/issues/172) | 配送・由来・本人の判断・継続を対応させる。互換移行を目的に複雑化しない。 |
| 時刻・自律起床 | [#128](https://github.com/sumi-studio/sumi/issues/128)、[#129](https://github.com/sumi-studio/sumi/issues/129) | 明示予定と、任意の自律確認を区別して進める。 |
| 道具・補助Agent | [#134](https://github.com/sumi-studio/sumi/issues/134)、[#81](https://github.com/sumi-studio/sumi/issues/81)、[#82](https://github.com/sumi-studio/sumi/issues/82) | 最初の成果物・長い仕事から必要範囲を選ぶ。 |
| 起動・障害波及 | [#344](https://github.com/sumi-studio/sumi/issues/344)、[#319](https://github.com/sumi-studio/sumi/issues/319)、[#339](https://github.com/sumi-studio/sumi/issues/339) | 以前の修正で全復旧が完成した扱いにしない。既存記述の現存条件を再確認する。 |
| 実行の費用 | [#205](https://github.com/sumi-studio/sumi/issues/205) | 課金プロダクトの完成を待たず、実行上限・残件・再開を設計する。 |
| 共同UI | [#112](https://github.com/sumi-studio/sumi/issues/112) | 認証した道具結果→画面→人の操作→本人の継続、まで通す。 |
| 回帰試験 | [#343](https://github.com/sumi-studio/sumi/issues/343)、[#346](https://github.com/sumi-studio/sumi/issues/346) | 実行と未実行を区別する。結果の意味を測る題材を加える。 |

この監査は、現在のsourceと代表的経路を広く読んだもの。全ての不具合を網羅した保証、全製品のセキュリティ監査、実デプロイの全設定調査ではない。特に各経路の現実の発生頻度、モデルごとの記憶品質、速度・費用の実測は今後の実装と受け入れで確かめる。
