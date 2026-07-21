# ADR 0003: provider正規化境界の復元可能性とM1責務補正

- Status: Accepted
- Date: 2026-07-20
- Amends: [実装計画](../agent/implementation-plan.md) §3.1〜§4.5、§7.8、§13 M1

## コンテキスト

CP1を取り込んだCP2実装を、protocol-neutral assembler、Chat adapter、再送、
overflow、transportの各境界から再検証した。レビュー指摘への追従ではなく、
「正規化event列だけで最終messageを復元できるか」「保存済みthinkingの由来を
再起動後にも証明できるか」「後段が回復動作を一意に選べるか」を反対仮説込みで
確認した結果、元の正典には実装で補えない情報欠落と責務時期の矛盾があった。

## 決定

1. `AssistantMessage`と`PublicAssistantMessage`に、非secretな
   `ProviderOrigin { provider_instance_id, protocol, model }`を保存する。平文thinkingは
   originが完全一致した送信先にだけ再送する。provider表示名やtrust domainは代用しない。
   instance IDはversion付き・各要素のbyte長付きで生成し、URL credential/query/fragmentを
   含めない。account差は明示的な非secret `account_scope`で区別する。`ModelSpec`には
   派生値をcacheせず、origin生成時にidentity入力から都度導出して設定変更との乖離を防ぐ。
2. `ProviderEvent::ThinkingStart`に`signature_field`を載せる。永続contentを作る
   `content_index`はflatten済みwire slotとし、`u32`へ表現不能ならfail-closedにする。
3. `ToolCallRejected`は安全化済みの合成`ToolResultMessage`を同じ内部eventで運ぶ。
   public stream変換はresultを落とし、AgentLoopはassistant確定後までbufferする。
   `finish_reason=length`の一括拒否は`IncompleteResponse`としてstrict成功と区別する。
4. `ModelSpec`の物理上限`max_output_tokens`と通常予算`default_output_tokens`を分ける。
   Sumiの通常値は16kで、request overrideは物理上限へclampせず範囲外を拒否する。
5. overflowはboolではなく回復時期を返す。provider errorと
   `Length + output=0 + usage>=99%`は同一Turnの即時回復、
   `Stop + usage>window`は応答確定後の`pending_apply`とする。
6. M1のTTFTはT8で`request_sent→最初のText/Thinking delta`と上位span接続口を作り、
   command受信時刻との接続・stdio表示・p95判定は実AgentLoopを持つT15で完了する。
   T8だけの暫定ループは作らない。
7. `compact_native`の共通結果型はResponsesを実装するT9で確定する。ChatだけのT8に
   未使用placeholderを作らない。HTTP 429 fixtureはSSEではなくJSON bodyとして保持する。
8. モデル世代を一括で同じthinking方言とみなさない。Kimi K3、K2.6、K2.7 Code、
   OpenCode Zen Goは別compatで、gateway値はlive fixtureなしに直APIから推測しない。
   2026-07-20のproduction request A/Bでは、OpenCode Goだけが文字列
   `tool_choice:"required"`付きtool requestをHTTP 400
   `invalid_request_error`(`param=null`)で拒否し、他条件が同一の省略時は
   tool call 1件、reasoning、2ターン目のtextまで完走した。この実測に限定して
   OpenCode presetは`"required"`だけを送信前拒否する。Kimi/GLM/Umansの既存透過、
   および未測定の`"auto"`・named/object形は変更しない。
9. SSE event上限を複数deltaで迂回できないよう、tool argumentの累積raw bufferも
   4MiBで制限する。超過は`TooLarge`としてrawを即時破棄し、通常の拒否対で閉じる。
10. Kimi strict判定はMoonshotAI/walle v0.1.13
    (`196bb0ca9c2f2271cfa9623108308f0780e411ee`)へ固定する。ただしGo実装を完全移植せず、
    MFJS意味論を保守的に証明できるsubsetと構造上限だけをRustで検査する。証明不能なら
    `strict:false`へ縮退し、§4.3のローカル凍結schema検証は維持する。
11. pre-launchで旧wire/transcriptとの互換対象は存在しないため、Chatのlegacy
    `delta.function_call`はmodern `tool_calls`へ合成せず明示拒否する。K3の
    `reasoning_effort`もlaunch時に確認済みの`max`だけを受理し、無効化や未知値を透過しない。
12. HTTP connect 30秒、response header待ち120秒、headers後のchunk間idle 120秒を
    別々に制限する。cancelは各待機で同時ready時にも優先し、machine-readableなHTTP/
    transport codeを表示文より先にretry/overflow分類する。
13. Chatのplain-string assistant viewへ複数の永続`Text` blockを射影するときは、
    wire順を保ち、内容の空/非空ではなくblock数を基準に各block間を`"\n\n"`で区切る。
    leading/middle/trailingの空block間にも各1個を入れ、空`Text` block自体は型として禁止しない。
    これは永続contentの変更や旧wire互換ではなく、block境界を持たないChat文字列で隣接語彙を
    誤結合しないためのadapter規則とする。
14. SSE eventが空行で閉じる前のtransport EOFは`unexpected_sse_eof`として分類し、
    表示文の正規表現に依存せず再試行可能にする。正常な`[DONE]`やfinish event後の
    terminal fuseとは区別する。
15. HTTP response由来の`http_413`とSSE error payload由来のnumeric `413`は、
    同じrequest-too-large信号として本文なしでも即時overflow回復へ分類する。また
    `context_window=0`はusage閾値を常時成立させる無効な物理能力値なので起動時に拒否する。
16. requestごとの応答状態は、実際に送るoutput token予算`T`から導出する
    `ResponseBudget`で累積制限する。v1式は
    `content_bytes=64T+1MiB`、`wire_bytes=6*content_bytes+1MiB`、
    `events=8T+256`、`preview_work_bytes=8*content_bytes`、
    `tool_calls=floor(T/8)+16`とする。全演算と`usize`変換はcheckedで行い、設定時に
    表現不能なら起動を拒否する。これはprovider tokenizerの完全な上界証明ではなく、
    正常な大規模応答を表現しつつ増幅を有限にする製品安全ポリシーである。
17. budgetは責務ごとに独立して強制する。transportはSSE framingを含む受信raw byteを
    exactに数え、Chat adapterはprotocol stateの累積content/event/tool数と
    partial JSON previewへ渡した累積workを数え、assemblerは正規化eventから永続contentへ
    入るbyteを別counterで再検証する。これらを加算して二重課金せず、どれか一つでも超過、
    checked演算失敗なら`response_limit_exceeded`でfail-closedにする。tool call単体の
    4MiB上限も別の局所防波堤として残す。Chat chunkは新規deltaだけのbounded overlayで
    全semantic counter/identityをpreflight後に一括commitし、finishもevent reserve後に
    drainする。budget失敗時はsemantic state/counter不変で、usage sidebandだけを分離保持する。
18. bounded通常event channelとは別にcapacity 1のpriority terminal laneを設け、
    `Error`/`Aborted`だけをそこへ送る。`Done`は通常lane上で先行delta/Endとの順序を守る。
    cancel観測後のconsumerは通常laneをpollせずpriority terminalを待ち、terminal受信時に
    両laneとqueued backlogをfuseする。streamは`Start`を必ず最初に返す。priority terminalは
    producer authoritative content snapshotを運び、consumer assemblerは既受信prefixとの
    非矛盾とreason/model/origin/budgetを検査して収束する一方、`Done`は全event完全一致を保つ。
    producerはeventをassemblerへ適用してから通常laneへ
    await送信し、cancel時はopen blockをローカルで閉じるため、飽和中断でもterminal messageは
    それまでに受信・正規化したpartial contentを失わない。
19. provider usageは成功時だけでなく、provider error、finish検証失敗、transport失敗、
    cancel/abortのterminalにも、それまでに受信した最後の値を保存する。未知値を推測して
    補完せず、未受信時だけzero defaultとする。
20. MFJSのnumber値はRustでJSONとして表現できるだけではstrict互換とみなさない。
    pinned walleが`encoding/json`の`any`へdecodeして`float64`として検査する意味論に合わせ、
    enum/`minimum`/`maximum`の全numberがbinary64へ値を変えず表現可能な場合だけ
    strictを維持する。たとえば`2^53`は受理し、`2^53+1`と`0.1`は`strict:false`へ落とす。
21. fixtureの事実性をprovenanceで分離する。既存Kimi/GLM fixtureとprovider固有
    finish reasonは公式形状に基づくsynthetic contract fixtureであり、live captureを
    装わない。OpenCode Zen Goは2026-07-20のcurl raw captureをsanitization前SHA-256付きで
    固定し、`reasoning_content`、usage/costの配置、`[DONE]`後cost trailerを保持する。
    `[DONE]`をcanonical terminalとし、後続trailerはfixtureには残すが正規化eventにはしない。
    Moonshot直API、Z.ai直API、Umansのraw captureと2ターンlive証拠はcredential不在のため
    未完了であり、T25 provider releaseのrelease-blocking gateへ明示的に移管する。

## 根拠

- 旧`ThinkingStart`からは必須の`signature_field`を復元できず、旧message型からは
  provider instance/protocolを復元できなかった。
- rejection resultを後段で再生成すると、raw引数や可変schemaを再参照し、
  T5の凍結・非漏洩境界を破る。
- 物理131kを通常defaultとして送る実装は、§7.8の16kガードを無効化する。
- piのoverflow分岐は`stopReason != stop`を即時回復にしており、
  成功Stopだけをdeferredにする。bool化するとこの差が消える。
- command受信はT15までproviderへ接続されず、T8単独では前半TTFTを測定できない。
- SSE event/tool単体上限だけでは、小さいdeltaを長時間送り続けるproviderが、
  content、Map、event backlog、partial JSON再parse workを無制限に増やせる。
- bounded通常laneだけでterminalを送ると、consumer停止中のcancelがqueue backlogの
  drainを待ち、1秒abort契約とpartial messageの確定を同時に満たせない。
- Goの`encoding/json`でschema numberが`float64`へ丸められる以上、Rust側の任意精度に近い
  `serde_json::Number`をそのまま「同じ検証意味論」と扱うことはできない。

## 棄却した代替案

- `message.model == spec.id`だけでthinkingを再送する: 同名モデルの別account、
  proxy、protocolを区別できない。
- provider/base_url/account/protocolを区切り文字だけで連結する: URL pathとaccount scopeの
  境界が衝突し得て、別instanceを同一と誤判定する。
- `provider_instance_id`を公開fieldへcacheする: base URLやaccount scope変更後の更新忘れで
  古いoriginを再利用でき、thinking再送境界を破る。
- `content_index`からsignatureやwire位置を推測する: event列に情報がなく復元不能。
- rejection resultをT15で作り直す: 凍結schemaの検証結果と安全化済みdetailsを失う。
- 物理上限へrequest値を黙ってclampする: 設定誤りを隠し、想定外の課金・遅延になる。
- T8に仮AgentLoopや`compact_native`の空実装を置く: 後続タスクで捨てる実装になる。
- walle `ValidateLevelStrict`のunknown keyword無視まで再現する: providerがconstraintを
  無視しても「検証成功」になり得るため、Sumiの意味論保持の証明には使えない。
- Go/CGO版walleをruntimeへ追加する: CP2の判定境界には依存・運用コストが過大で、
  false-negativeは明示`strict:false`とローカル検証で安全に縮退できる。
- legacy `function_call`へ架空のtool call IDを付けて受理する: 後続tool resultのwire方言まで
  実装しない合成はdurable transcriptに偽のidentityを残し、未使用の後方互換になる。
- 複数Text blockを区切りなしで連結する: block境界の消失により`"foo"`と`"bar"`が
  `"foobar"`へ変わる。content-block配列は互換性問題があり、assistant messageを分割すると
  tool callとの同一message関係を変えるため、Chat viewだけの明示区切りを採用した。
- 途中SSE EOFを表示文だけでretry判定する: transport側の文言変更で回復性が失われる。
  transportが付ける安定machine codeを分類正本にする。
- 全modelに固定4MiBの応答総量上限を置く: Kimi K3の物理output上限と矛盾し、
  有効な設定を起動時から表現できない。
- 常にmodel物理上限からbudgetを作る: 通常16k requestにも最大値相当の増幅余地を与え、
  `default_output_tokens`分離の目的を失う。
- transport byte上限だけに依存する: 小さいwireでevent/tool slotやpreview再parse workを
  増幅する経路を止められない。逆にadapter/assemblerだけではSSE framingや未知fieldを
  含むraw bodyを有限にできない。
- provider tokenizerでbyte上限を完全証明する: tokenizer/version/JSON escape/プロキシ差に
  依存してM1で安定した証明ができないため、明示式を持つ保守的な安全ポリシーを採る。
- 通常laneへterminalをawait送信する、または全terminalをpriority化する: 前者は飽和時に
  cancel不能、後者は成功`Done`が先行deltaを追い越す。priorityは異常終端だけに限定する。
- cancel時にpartialを捨てる: ハードステアで部分応答を保持する製品契約に反する。
- MFJS numberを`as_f64()`可能なら互換とする: 丸め後の値が有限でも元JSON値と異なり得る。
- credentialなしにlive captureを合成・推定する: provenanceを偽り、gatewayと直APIの
  方言差を検出できない。未取得証拠はnamed release gateとして残す。

## 影響

正典、`TASKS.md`、provider型、serdeテスト、Chat送受信snapshot、overflow分類テスト、
fixture結合テストを同じPRで更新する。wire/persistence型の追加はpre-release中に
必須化し、由来不明の旧thinkingを安全と仮定するdefaultは設けない。response budgetは
config検証、transport、全adapter、assemblerへ同じrequest token予算から渡すため、T9/T10も
この境界を再利用する。T25はprovider context durable round-tripに加え、Moonshot/Z.ai/Umans
直APIのraw capture provenanceとlive 2-turnを満たすまで完了扱いにしない。
