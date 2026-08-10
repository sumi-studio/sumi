# Real dogfood restart smoke

`apps/web/e2e/dogfood-restart.spec.ts`はmock serverではなく、canonical HTTPS
origin、real browser session、real Postgres/API/Tunnelを使う。次を検証する。

- API process restartでlive messaging socketが実際に閉じる。
- observerが切断中、復旧済みAPIへ別browser clientがcommitしたmessageを、保存
  cursorからのreconnectが一度だけ受け取り、`caught_up` barrierへ到達する。
- named Tunnel connector restartでも同じ収束をする。
- 最初の成功receiptを利用せず、同じ`client_nonce`をretryすると200の元receiptを
  返し、history上のdurable rowが一つだけである。

これはtyping等のephemeral event保全を要求しない。またdirect-chat runtimeの
restart replay、UIのvisual offline表示、network packet levelでのresponse切断は
このspecだけでは証明しない。

## Inputs

```text
SUMI_DOGFOOD_SMOKE_BASE_URL=https://<canonical-host>
SUMI_DOGFOOD_SMOKE_STORAGE_STATE=/absolute/protected/playwright-storage-state.json
SUMI_DOGFOOD_SMOKE_PLACE_ID=<real place visible to that Human>
SUMI_DOGFOOD_RESTART_API_HELPER=/absolute/protected/restart-api-helper
SUMI_DOGFOOD_RESTART_TUNNEL_HELPER=/absolute/protected/restart-tunnel-helper
```

storage stateは実Humanのsession cookieを含むためrepository外、mode 0600にする。
二つのhelperもrepository外、mode 0700とし、引数なしで対象一つだけをrestartして
readyになるまで待つ。API helperはexactly one APIというdeployment contractを
崩さず、Tunnel helperはnamed connectorだけをrestartする。helper自身にsecretを
echoさせない。

```bash
./deploy/dogfood/smoke/run.sh
```

専用runnerは一つでも入力が無ければ`NOT COVERED`を出してexit 2にする。通常の
Playwright suite内では未設定specはskipされるが、その緑をdogfood acceptanceの
成功として数えない。実runのtimestamp、app SHA、helper revision、browser、三つの
test結果をcutover evidenceへ保存する。
