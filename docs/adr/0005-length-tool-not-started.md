# ADR 0005: Length停止ToolCallの未開始監査

- Status: Accepted
- Date: 2026-07-22
- Amends: [実装計画](../agent/implementation-plan.md) §5.1、§10.1〜§10.2 と [T15](../../apps/agent/TASKS.md)

## コンテキスト

`StopReason::Length`でも、providerが出力済みのToolCallはstrict JSON/schema検証に通り得る。
公開assistantにはその事実を残す一方、応答全体の意図は未完了なので実行してはならない。
既存`tool_executions`では`failed`は`started_at`必須、`cancelled`はユーザー/制御起因の停止を表し、
一度も開始していないLength guardを真実に記録できなかった。

## 決定

pre-launch schemaへterminal `not_started`状態と列挙済み`length_guard` error codeを追加する。
EventWriterのprivate `Skip` mutationだけがcanonical assistant ToolCall originとlive owner/run/turnを検証し、
`started_at=NULL`、terminal `finished_at`の監査行を直接作る。同じEventBatchに対応する
`is_error=true` ToolResultの`MessageStart/End`とMessageEnd projectionを必須とする。
公開`ToolExecutionStart/End`、Prepare/Start、approval、executor callは作らない。

Sessionのprivate metadata-bound bridgeは恒久eventをEventWriterへcommitし、返されたexact seqを付けてから
Gatewayへ渡す。volatile deltaは対応するdurable start後だけ許す。T17所有のprovider contextが非空なら、
保存経路がないT15では黙って捨てずfail-closedでhandoffする。

## 棄却した代替

- `failed`: 開始時刻を捏造し、executor failureと混同する。
- `cancelled`: user/abort/steer起因という意味とerror codeを偽る。
- `RejectedToolCall`: valid callを引数検証失敗へ改変する。
- 架空の`ToolExecutionStart/End`: 存在しない副作用境界を公開・監査ログへ残す。
- orphan-result validationの緩和: 無関係なToolResultの混入を許す。

## 影響と境界

T15はidle run/turn/owner、message、retry、通常tool、Length未開始の最小bridgeを所有する。
active abort/steer/owner transferはT16、provider context永続化・DeliveryPump・完全復旧はT17、
実approvalはT22/T23のまま変更しない。旧schema/wire利用者は存在しないため互換層は追加しない。
