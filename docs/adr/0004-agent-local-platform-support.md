# ADR 0004: エージェントのローカル platform support

- Status: Accepted
- Date: 2026-07-21
- Amends: [実装計画](../agent/implementation-plan.md) §8.1、§8.3 と [T13](../../apps/agent/TASKS.md)

## コンテキスト

Sumi product の workspace/Cloud は、Linux の container、cgroup、PID/network namespace、
`openat2` 等を安全性と停止保証の基盤にしている。一方、将来の OSS ローカル版には、同じ
Cloud gate を満たさないことを明示した low-trust fallback として手元の Unix host から
bash を実行できる価値がある。既存実装も Linux では process group、非 Linux Unix では
Tokio の `child.kill()` を使い、native 非 Unix では Protocol error で fail-closed にする
境界を持つ。

旧正典の「非 Linux ローカル fallback」という表現は、macOS 等の Unix と native Windows
等の非 Unix を区別せず、native Windows まで対応済みまたは対応予定と読めた。native Windows
上の bash、単一順序付き stdout/stderr pipe、signal/cancellation、FD 継承を Linux/Unix と
同じ契約で実装・検証した証拠はない。

## 検討した選択肢

1. **native Windows 専用実装を追加する**。Windows 上の bash 選定、stdout/stderr を順序付きで
   合流する pipe、process tree の停止、handle 継承、環境・workspace 境界を別実装し、同じ
   tool contract を満たす。実装面積と platform 固有の検証面積が増え、現在の Linux product
   workspace を成立させる目的には寄与しない。
2. **stdout/stderr を別 pipe のまま黙って縮退する**。native Windows でも起動だけは可能になるが、
   §8.3 の時系列を保つ単一ストリーム契約を破り、実行結果と streaming update の意味が platform
   によって変わる。未検証の停止・handle 継承境界も隠す。
3. **native 非 Unix を明示的に非対応とし、WSL/Linux を案内する**。product と同じ Linux 経路を
   再利用し、未検証の互換層を追加しない。非 Linux Unix の OSS ローカル実行だけを、明示的な
   low-trust fallback として残す。

## 決定

選択肢3を採用する。

- Sumi product の workspace/Cloud support target は Linux とする。
- OSS ローカル fallback は macOS 等の非 Linux Unix host を support target に含める。ただし
  `child.kill()` による停止は process tree 全体を保証しない low-trust 経路であり、起動ログと
  テスト結果へ明示する。Cloud acceptance の代替証拠にはしない。
- native 非 Unix host は Protocol error で fail-closed にする。利用者は WSL または Linux を使う。
- native Windows 用 bash/merged-pipe/停止実装は追加せず、検証済みとも表記しない。

## 製品目的との整合

Sumi の product purpose は、ユーザーごとの Linux workspace を安全に隔離し、bash と file tool を
同じ executor 境界で動かすことにある。Cloud release gate は cgroup/sandbox による descendant
一括停止、mount/network 分離、Linux の fd-relative path policy を要求する。native Windows の
互換実装はこの release gate を閉じず、pre-launch で既存 native Windows 利用者との互換対象もない。
WSL/Linux は product の実行意味を変えずに Windows host から利用できる経路である。

## 影響

- Linux Cloud と T26 の deployment/resource 境界は変更しない。
- Linux low-trust local の process-group fallback と、非 Linux Unix の `child.kill()` fallback は
  開発・OSS ローカル用途に限定される。
- native 非 Unix では bash tool を部分動作させず、明示エラーになる。stdout/stderr の順序や停止を
  platform ごとに黙って縮退させる compatibility layer は持たない。
- native Windows を実行したというテスト証拠は主張しない。

## 再検討条件

native Windows が独立した product support target になり、実利用需要と保守 owner が確定した時点で
再検討する。その際は、bash 配布/選定、workspace path、単一順序付き stdout/stderr、process tree
停止、handle 継承の fail-closed、環境 allowlist、timeout/cancel/output quota の同等性を native
Windows CI で自動検証し、Linux/Unix 経路と同じ tool contract を満たす証拠を新しい ADR に残す。
