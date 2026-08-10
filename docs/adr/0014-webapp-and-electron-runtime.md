# ADR 0014: canonical WebApp・Electron desktop・共有browser runtime

- Status: Accepted
- Date: 2026-08-10
- Supersedes:
  - [ADR 0001](0001-frontend-stack.md)のTauri desktop/mobile shell、`src-tauri`、native mobile packaging、通知統合の選定
- Preserves:
  - [ADR 0001](0001-frontend-stack.md)のReact / Vite / SDUIを一つのrenderer正本にする判断
  - [ADR 0013](0013-tool-invocation-routes-and-authority-provenance.md)のinvocation route、AutoReview、Human Approval、authority provenance
- Related:
  - [#203](https://github.com/sumi-studio/sumi/issues/203)
  - [#204](https://github.com/sumi-studio/sumi/issues/204)
  - [#238](https://github.com/sumi-studio/sumi/issues/238)
  - [#246](https://github.com/sumi-studio/sumi/issues/246)

## Context

Sumiは、HumanとPersonalityAgentが同じapplicationを同じように使い、同じ操作結果を
認知できることを根に置く。将来はSumi自身がHuman向けbrowserを内包し、Browser Useでも
Humanが見ているtab・account・page stateを必要なauthorityの範囲でAgentが使う。

旧ADR 0001は、同一rendererをWeb・desktop・mobileへ配るshellとしてTauri 2を選んだ。
しかしTauriはOSごとのsystem WebViewを使うため、Human向けbrowserと汎用Browser Useに
Chromium相当を必要とする段階では、Sumi shellとは別にbrowser engineとprocess treeを
同梱する可能性が高い。これは同じpageをHumanとAgentが扱う契約を複雑にし、renderer、
browser profile、automation、更新、memoryの二重管理を生む。

2026-08-10時点で`src-tauri`、Tauri dependency、native shell実装は存在しない。したがって
今の変更は実装migrationではなく、将来二重化する前のarchitecture correctionである。

## Decision

### 1. `apps/web`を唯一のapplication renderer正本にする

React 19 + TypeScript + Vite + SDUIで作る`apps/web`を、hosted Web、mobile WebApp、
Electron desktopで共通する唯一のrenderer実装にする。同じscreen、domain operation、
loading/error/reconnect、Approval、attachmentの認知体験を使い、Electron専用のapplication
forkを作らない。

mobileの現在のproduct surfaceはresponsive WebAppである。Tauri mobile、Capacitor、React
Native等のnative mobile shellは選定しない。APNs専用機能やLive Activitiesを将来必要と
する場合は、WebAppとdesktop shellへ暗黙に混ぜず、別ADRでnative companionの責務を決める。

### 2. desktop shellと将来のbrowser chassisはElectronにする

desktop deployableは将来`apps/desktop`へ置き、Electronのmain process、preload、packaging、
OS integration、browser surfaceを所有する。`apps/web`の同じbuild artifactをbundleし、
canonical hosted URLを開くだけのthin wrapperにはしない。bundled assetは`file://`ではなく、
secure / standardとして登録したapplication専用protocolから配信する。

`apps/desktop`はdomain DB、Workspace/membership/role、app operationの意味、application
authorization、agent policy、AutoReviewを所有しない。main/preloadはOSとChromiumをapp-owned
operationへ接続するadapterであり、domain commandはWeb版と同じAPI・result・commit-time
authorizationへ合流する。

desktop authはsystem browser + PKCE / one-time handoffを入口にし、長命credentialをrendererへ
露出しない。credential custody、refresh、desktop transportの正確なwireは実装Issueで凍結するが、
browser cookieのOrigin/CSRF境界を偽装・緩和しない。

### 3. trusted Sumi rendererと任意Web contentを分離する

Sumi shell rendererとHuman向けbrowser tabは、一つのElectron/Chromium process treeを利用しても
同じsecurity principalにはしない。

- Sumi rendererはbundled codeだけを読み、`nodeIntegration: false`、`contextIsolation: true`、
  renderer sandboxを維持する。preloadはversionedで狭いtyped APIだけを公開する。
- 任意Web pageはmain processが管理する`WebContentsView`へ置く。`<webview>`を正本にせず、
  remote pageへNode、Electron API、Sumi preload、raw IPCを公開しない。
- remote contentを読む全Electron `Session`にはpermission request handlerとpermission check
  handlerの両方を置き、site permissionはdeny-by-defaultでbrowser appが明示管理する。
  navigation、popup、download、
  external protocolもbrowser appのoperationとして検証する。
- Sumi shellとHuman browser profileのcookie/cache/storage partitionは分離する。Human browserの
  複数tabは同じpersistent Electron `Session`をprofile単位で共有し、同じlogin stateを使える。
  memory節約を理由にSumi credentialと任意site cookieを一つのpartitionへ混ぜない。
- main/preload IPCはsender、frame、origin、message schemaを検証する。rendererへ汎用IPC、filesystem、
  shell、Electron objectを渡さず、remote pageにはprivileged preload自体を置かない。

Sumi BrowserはallowlistされたSumi Appだけでなく、Humanが使う一般のHTTPS Webを対象にする。
したがってSumiは、任意Web contentのsandbox、site permission、download、credential custody、
navigation safety、Chromium security updateを継続的に担うbrowser vendor相当の責任を引き受ける。
これはElectronを採る理由の副作用ではなく、本決定に含まれるproduct responsibilityである。

### 4. Browser Useは別browserを模造せず、同じtabをapp operationとして扱う

browser appがtab、profile、navigation、site permission、download、page-state reviewの意味を
所有する。agent foundationはそれをtool operationとして受け、ADR 0013のpolicy、Execution /
Escalation AutoReview、exact-call Approval、authority provenance、auditを追加する。どちらも相手の
責務を吸収しない。

browser domainにはdurableな`BrowserProfileId`とbrowser-owned `TabId`を置き、Electronの
一時的な`webContents.id`や`Session`名をdomain identityにしない。Human browser profileは
Participantに属し、`AgentOwn`の閲覧をHuman profileへ暗黙合流させない。

`HumanAccountOneShot`でHumanのaccountを本当に使う操作は、Approvalで束縛したexact device・
browser profile・tab・origin・document/navigation epoch・target effect・expiry・nonceに対して、
Humanが見ている同じElectron `WebContents`で実行する。
cookieをagent VMへcopyしたり、同じaccountでhidden automation browserを別に立てたりしない。
Humanは同じtabで操作、途中状態、結果を観測し、必要なら介入できる。`AgentOwn`の操作までHuman
profileへ暗黙合流させず、能力の出所はADR 0013どおり別に保つ。HumanとAgentの入力はbrowser
appが短いcontrol leaseで直列化し、Humanの操作・取消を常に優先する。承認後にnavigation epoch、
profile、tab、effectが変わればcommitせずfail closedにする。

同じtabを使えることは、AgentがHuman profileを常時観察できることを意味しない。screen、DOM、
accessibility tree、form value、historyの取得もauthorityを要する。Humanがそのtabを見せる操作、
または明示したread capabilityをbrowser appが検証した後だけfoundationへsanitized observationを
渡す。exact mutationのEscalation AutoReviewとHuman Approvalは、その後の別判断として保つ。

Human profileのsame-tab Browser Useは、そのprofileとtabが存在するHumanのdesktop device上で
実行する。cloud側で同じcredentialを複製して「同じtab」と呼ばない。deviceがofflineまたは
tab bindingを再検証できない場合は、別browserへfallbackせず操作を成立させない。

Electronの`webContents.debugger` / Chrome DevTools Protocolはbrowser adapter内部の実装詳細に
留める。raw CDP、cookie、Node/Electron capabilityをLLM、Sumi Web renderer、remote pageへ渡さない。
将来WebMCP等のsite-declared operationを使う場合も、同じbrowser app operationへ正規化し、
app authorizationとfoundation safeguardを迂回しない。

### 5. 共有できるresourceと、共有してはいけないstateを区別する

Sumi shellとbrowser tabsは、一つのElectron application内でChromiumのbrowser/main、GPU、
network/utility process、実行code page等を共有できる。別Chromiumを同梱するより固定overheadを
減らし、同じ`WebContents`をHuman/Agentで使うことでpage stateの二重化も避けられる。

一方、各`BrowserWindow` / `WebContentsView`は原則として別renderer processとJS heapを持つ。
Electron採用は「browser tabの追加costがゼロ」や「Chromeより常に軽い」という約束ではない。
background tabのdiscard/suspend、process reuse、cache上限は実測して実装するが、activeなHuman
tabをmemory都合で別tabへ再現し、認知状態を失わせない。

### 6. Electronをfull Chrome互換の約束にしない

Sumi browserはSumiのapplicationとHuman/Agent協働に必要なbrowser chassisである。Electronは
Chrome Extension APIのsubsetしか提供せず、Chrome Web Storeの任意extension互換を目的にして
いない。将来、一般browser置換やChrome extension ecosystemがproduct requirementになった場合は、
Electronを無理にChrome化せず、Chromium/CEF等を含む別のruntime判断を行う。

### 7. delivery topologyと実装順

- hosted WebAppはCloudflare Workers Static Assets + Worker Route → named Tunnel → canonical APIを
  使う。Electronはbundled rendererから同じcanonical APIへ接続する。
- LiveKit media / TURNのdirect pathはこの変更の影響を受けず、Worker/Tunnelへproxyしない。
- Developer Workspace dogfood cutoverの自律GoalへElectron完成を混ぜない。ただしWeb/domain
  implementationはbrowser globalsを正本にせず、browser/desktop adapter seamを壊さない。
- core Goal後のdesktop handoffはTauriではなくElectronを対象にする。Kazuiは`apps/desktop`、
  bundled renderer、auth/transport、native notification/file integration、browser chassisの順に
  実装し、Hosted Webと別のdomain/UIを作らない。
- responsive mobile WebAppは自律Goalの外で、core実装後にHumanが実viewport/実機を観測しながら
  仕上げる。desktop-only productへ縮退したという意味ではない。

## Consequences

### 利点

- Human向けbrowserとBrowser Useが同じengine・tab・profile・page stateを使える。
- hosted Webとdesktopでrendererを一つに保ちながら、desktopとbrowserの固定Chromium overheadを
  一つのprocess treeへまとめられる。
- Tauri + 別Chromiumという二重runtime、二重update、二重automation seamを実装前に避けられる。

### 引き受けるcost

- desktop配布sizeとbaseline memoryはTauriより増える。
- Chromium/Electronのsecurity update、code signing、auto-update、crash recovery、sandbox回帰を
  Sumiのrelease責務として継続する。
- arbitrary remote contentを同じdesktop applicationで扱うため、renderer/session/IPC境界の不備は
  高影響になる。Electron security checklistをrelease gateにする。
- native mobile shell、APNs専用integration、Live Activitiesは現在の決定から外れ、必要時に別途
  product/runtime判断が要る。

## Implementation follow-ups (decisionを再度開かない)

- `apps/desktop`のpackage/update/signing方式とsupported desktop OS matrix
- signed automatic update、staged rollout、rollback、緊急Chromium security releaseのSLA。
  一般Webを有効化する前にrelease gateとして固定し、古いChromiumをWeb UIだけの更新で隠さない
- desktop auth credentialとbrowser profile dataの暗号化・backup・logout/erasure
- tab lifecycle、profile UI、download/site-permission UI、crash restore、memory budget
- browser operation schemaとexact tab/effect binding、Human intervention、CDP/WebMCP adapter tests

これらは実装詳細としてIssueで決める。WebAppをrenderer正本にすること、desktop/browser runtimeを
Electronへ統一すること、Sumi rendererとremote pageを分離すること、Human-account操作で同じ
Human tabを使うことは再度の選定対象にしない。

## References

- [Electron process model](https://www.electronjs.org/docs/latest/tutorial/process-model)
- [Electron Web Embeds](https://www.electronjs.org/docs/latest/tutorial/web-embeds)
- [Electron Session](https://www.electronjs.org/docs/latest/api/session)
- [Electron Security](https://www.electronjs.org/docs/latest/tutorial/security)
- [Electron Sandboxing](https://www.electronjs.org/docs/latest/tutorial/sandbox)
- [Electron webContents / Debugger](https://www.electronjs.org/docs/latest/api/web-contents)
- [Electron Chrome Extension Support](https://www.electronjs.org/docs/latest/api/extensions)
- [Electron Release Timelines](https://www.electronjs.org/docs/latest/tutorial/electron-timelines)
- [Electron autoUpdater](https://www.electronjs.org/docs/latest/api/auto-updater/)
