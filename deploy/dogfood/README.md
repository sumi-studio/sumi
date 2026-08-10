# Developer Workspace dogfood deployment

このディレクトリは、開発WSLから分離した専用Linux hostへ、Developer
Workspaceの最初のdogfood originを再現するためのversioned contractである。
HAやzero-downtimeを装うものではない。短い停止は許容し、single originを
正しく停止・復旧できることと、守るデータを曖昧にしないことを優先する。

```text
https://<canonical-host>/*
  Cloudflare Worker Route
    ├─ SPA / static assets -> Workers Static Assets
    └─ /auth/*, /direct-chat/*, /messaging/*, /health
         -> named Cloudflare Tunnel -> api:8080

dedicated origin host
  cloudflared -> Go API (exactly one, stop-first) -> Postgres
                         |-> command/runtime state + attachment disk
                         |-> runtime provisioner -> PersonalityAgent runtimes
```

Browserから見えるoriginは一つだけである。別API hostname、credentialed CORS、
Quick Tunnel、Vite dev server、public API/Postgres portは使わない。`/agent/ws`は
runtime用の内部経路、`/local-control/v1/*`はUnix socket側の経路なのでpublic
edgeは404にする。公開`/health`はprocess livenessだけを返し、dependencyを含む
`/ready`はedgeから隠す。

## 現在このrepositoryが保証するもの

- `compose.yaml`はdigest固定image、永続Postgres volume、明示的state bind、
  single API、stop-first deploy、dependency readiness後のTunnel起動を固定する。
- `deploy-origin.sh ... --check`は入力とrendered Composeを検証するだけで、pull、
  migration、restart、deployを行わない。
- 実deployは旧APIを完全停止してwriteを閉じてからmigrationを適用し、新APIを
  一つだけ起動する。forward migration適用後に旧binaryへ暗黙rollbackしない。
- APIは全responseを`Cache-Control: no-store`にする。Workerは通常のorigin
  `Request`/`Response`を組み直さず、Tunnel/origin不達だけをno-store 503へ
  正規化する。
- edge allowlistとGo public route registrationのparity testがある。未知の
  dynamic prefixをoriginへ通さない。
- static stagingはMCP App sandboxとsymlinkを拒否する。

これは実環境の稼働証拠ではない。以下の外部入力、実deploy、実browser smoke、
復元rehearsalが揃うまで「dogfood ready」やIssue完了として扱わない。

## 外部で確定・用意する入力

1. canonical hostnameとCloudflare zone。
2. named Tunnel、token、proxied DNS record。Tunnelのremote ingressはcanonical
   hostnameを厳密に`http://api:8080`へ向け、最後のfallbackを404にする。
3. 同じmain commitからbuildしたweb artifact、API/provisioner/agent image。
   Compose imageはregistry digest、agent runtime tagはexact `SUMI_APP_SHA`を使う。
4. 開発WSLとstorageを共有しない専用Linux host、永続disk、Docker Engine。
5. Firebase project/ADC、browser/agent signing secret、provider credential、private
   registryを読むDocker `config.json`。registry credentialはprovisionerだけへ
   read-only file mountし、APIやAgent runtimeへ渡さない。
6. #238で決めるPush/LiveKitの配置とcredential。三つのLiveKit値は揃うまで
   すべて空にし、部分設定しない。
7. off-host backup destination、暗号鍵custody、schedule、retention、alert owner。

## host preparation

`tmpfiles.conf`を`/etc/tmpfiles.d/sumi-dogfood.conf`へmode 0644でinstallし、boot
ごとに`/run/sumi/*`を正しいowner/modeで作る。初回は次も実行する。

```bash
sudo systemd-tmpfiles --create /etc/tmpfiles.d/sumi-dogfood.conf
sudo ./deploy/dogfood/prepare-host.sh /srv/sumi/dogfood
```

`prepare-host.sh`は既存dataを削除しない。state root、operator env、secret fileは
repository外に置く。`operator.env.example`をcopyし、全placeholderを置換して
mode 0600にする。四つのsecret fileもabsolute regular non-symlink、mode 0600に
する。`SUMI_DB_URL`のpasswordはPostgres secret fileと同じ値をpercent-encode
したものにする。Docker configは`config.json`というfile名で、credential helper
ではなく、専用hostで`docker login ghcr.io`が書いたinline `auth`を含める。同じ
configをhost側のdigest pullとprovisioner内のAgent image pullに使う。state rootはroot-owned
0711（APIが配下へtraverseできるが列挙・
書込は不能）、実data directoryはUID/GID 65532の0700に固定される。Docker
context名を明示し、そのcontextが現在hostのlocal Unix
socketを指さなければpreflightは失敗する。SSH/TCP remote contextで開発machine
からdogfood stateを操作しない。実deployとbackupは同じ
`SUMI_DOGFOOD_OPERATION_LOCK`をnon-blockingで取り、migrationとsnapshotを同時に
開始しない。operator env、secret、lockがroot-owned 0600なので、実deployとbackup
jobは専用hostのroot-owned unitまたは同等のroot operatorとして実行する。

専用host上で、対象Docker contextを確認してからpreflightする。

```bash
./deploy/dogfood/deploy-origin.sh /etc/sumi/operator.env --check
```

preflightはDocker daemonやCloudflareを変更しない。実deployは明示的な運用操作
として次を実行する。

```bash
./deploy/dogfood/deploy-origin.sh /etc/sumi/operator.env
```

新APIが`/ready`にならなければcloudflaredをrecreateせず失敗する。migrationが
既に前進している可能性があるため、旧imageへ戻すのではなく原因を直した新しい
forward deployを作る。

## edge artifact and deploy

production web buildでは`VITE_API_BASE_URL`とpreissued auth modeを設定しない。
同じ`SUMI_APP_SHA`のclean checkoutでbuildし、新しいstaging directoryを使う。

```bash
pnpm --filter @sumi/web build
./deploy/dogfood/edge/stage-assets.sh apps/web/dist deploy/dogfood/edge/dist
SUMI_CANONICAL_HOST=workspace.example.com \
SUMI_CLOUDFLARE_ZONE=example.com \
SUMI_APP_SHA=<exact-main-sha> \
node deploy/dogfood/edge/render-config.mjs
```

edge uploadはCloudflare credentialを持つoperator/CIだけが行う。Wranglerはversion
をpinし、versionとdeployment IDを運用記録へ残す。2026-08-10時点で検証対象の
CLIは`wrangler@4.114.0`である。

```bash
cd deploy/dogfood/edge
pnpm dlx wrangler@4.114.0 deploy --config wrangler.generated.json
```

deploy後に、routeがWorker **Route**でありCustom Domainではないこと、named
Tunnelのingress、DNS、公開404境界、二本のbrowser WebSocket、login/logout、
dynamic no-storeを実originで確認する。

## cutover and recovery gates

- 最初の実team messageより前に、`cutover-record.template.json`を実値で埋めた
  recordをversion管理する。それ以後、記録済みmigrationをrename/rewriteせず、
  新しいforward migrationだけを追加する。API readinessはapplied migrationの
  nameとSHA-256をembedded manifestへ照合する。
- `backup/README.md`どおり、Postgresとattachment treeを一つのsnapshotとして
  off-hostへ送り、空のscratch DB/rootへ復元する。
- `smoke/README.md`の専用runnerでAPI restart、Tunnel restart、cursor catch-up、
  same-nonce replayを実施する。入力不足によるskipはacceptance successではない。
- command log、runtime state、PersonalityAgent private volumeはmessaging snapshotの
  対象外である。別のrecovery contractなしに「Sumi全体をbackup済み」と言わない。

## repository checks

```bash
node --test deploy/dogfood/edge/*.test.mjs \
  deploy/dogfood/*.test.mjs \
  deploy/dogfood/backup/*.test.mjs \
  deploy/dogfood/smoke/*.test.mjs
bash -n deploy/dogfood/*.sh deploy/dogfood/edge/*.sh \
  deploy/dogfood/backup/*.sh deploy/dogfood/smoke/*.sh
```

References: [Workers Routes](https://developers.cloudflare.com/workers/configuration/routing/routes/),
[Workers Static Assets routing](https://developers.cloudflare.com/workers/static-assets/routing/worker-script/),
[Cloudflare Tunnel WebSockets](https://developers.cloudflare.com/cloudflare-one/faq/cloudflare-tunnels-faq/).
