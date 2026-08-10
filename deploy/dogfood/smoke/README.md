# Real dogfood recovery smoke through the shipped WebApp

`apps/web/e2e/dogfood-restart.spec.ts`はmock serverやtest専用socket clientではなく、
canonical HTTPS origin、二つのreal browser context、shipped Messaging UI、shipped
Direct Chat UI、real Postgres/API/Tunnelを使う。

## What it proves

- Messaging UIがconnectedからreconnectingへ遷移し、API restart後にconnectedへ戻る。
- named Tunnel connector restartでも同じUI上の遷移と収束を示す。
- Direct Chat UIがconnected/readyからunavailableへ遷移し、API restart後に
  connected/readyへ戻る。
- observer browserをofflineのまま残し、復旧した別browser clientがshipped composer
  からcommitする。observerを戻すと、MessagingとDirect Chatそれぞれが自分のdurable
  cursorからそのoutage中commitを一度だけ表示する。
- 補助的なlower-level checkとして、成功receiptを捨てた後に同じMessaging
  `client_nonce`をretryすると元receiptを返し、durable historyにrowが一つだけ残る。

DOMへ付けた`data-sumi-surface`とconnection/ready attributeは、WebAppが実際に描画
している状態をautomationから安定して観測するためのものだ。restart/catch-upの主証拠を
raw `WebSocket`や直接REST送信だけで代用しない。

typing等のephemeral event保全、packet単位のnetwork fault、multi-host failover、HA、
zero-downtimeはこのsmokeの主張に含めない。

## Inputs

```text
SUMI_DOGFOOD_SMOKE_BASE_URL=https://<canonical-host>
SUMI_DOGFOOD_SMOKE_STORAGE_STATE=/absolute/protected/playwright-storage-state.json
SUMI_DOGFOOD_SMOKE_PLACE_ID=<real place visible to that Human>
SUMI_DOGFOOD_SMOKE_MESSAGING_PATH=/c/<real-route-key>
SUMI_DOGFOOD_RESTART_API_HELPER=/absolute/protected/restart-api-helper
SUMI_DOGFOOD_RESTART_TUNNEL_HELPER=/absolute/protected/restart-tunnel-helper
```

Messaging pathは同じplaceを開くshipped route（`/c/*`、`/dm/*`、`/group/*`）を指定する。
storage stateは実Humanのcookieを含むためrepository外、mode 0600にする。helperも
repository外、mode 0700、absolute regular non-symlinkとし、引数なしで対象一つだけを
restartしてdependency-readyになるまで待つ。API helperはexactly one API、Tunnel
helperはnamed connectorだけを対象にし、secretを出力しない。

```bash
./deploy/dogfood/smoke/run.sh
```

専用runnerは入力が一つでも無ければ`NOT COVERED`を出してexit 2にする。通常の
Playwright suiteでは未設定specがskipされても、その緑をdogfood acceptanceの成功に
数えない。実runのtimestamp、app SHA、helper revision、browser version、各test結果を
cutover evidenceへ保存する。

repository上のcontract testとPlaywright discoveryは、real originで上記runnerを完走
した証拠ではない。API/Tunnel helperと認証状態を投入した実runまでgateはpendingである。
