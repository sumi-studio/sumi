# ADR 0002: エージェント基盤の言語と実装方針

- Status: Accepted
- Date: 2026-07-17
- Amends: [ADR 0001](0001-frontend-stack.md) — 前提5(pi が TypeScript)を無効化し、`apps/agent` の実装言語(TS → Rust)と `@sumi/api-client` 経由のドメイン操作(→ 契約ファーストの Rust `apiclient`)を置換する。ADR 0001 のフロントエンド選定と「権限モデルの強制点を API 層に保つ」原則自体は有効なまま

## コンテキスト

ADR 0001 は前提の一つとして「エージェント基盤の有力候補 (pi) が TypeScript」を挙げていた。pi ([earendil-works/pi](https://github.com/earendil-works/pi/tree/216e672e)、旧 badlogic/pi-mono) の詳細調査 (2026-07-17) の結果、前提が変わった。

1. **Sumi の核となる要件は pi に存在しない**。3層メモリ ([docs/agent/memory.md](../agent/memory.md)) に相当する長期記憶機構はなく、権限承認フローは「組み込みの権限システムは存在しない」と公式に明言され (思想としても YOLO モードがデフォルト)、ステアはターン境界での注入のみで生成中の割り込みではない。つまり言語を何にしても、Sumi を差別化する部分はすべて自作になる。
2. pi は本質的にローカル 1 ユーザーのコーディングエージェント向け設計であり、作者自身のマルチユーザー実例 (pi-chat) もユーザーごとにプロセス/microVM を分離する構成をとる。これは Sumi のワークスペース設計 ([docs/agent/workspace.md](../agent/workspace.md)) と同型で、参照価値が高い。
3. pi の真の価値は LLM 配管 (pi-ai) の完成度と、イベント駆動ループの設計、および 1 年分の運用細部 (ツール結果の切り詰め、ストリーミングイベント設計、reasoning の往復処理等) にある。
4. Sumi のモデル調達は Kimi / GLM / Umans 等の OpenAI 互換 API に加え、OpenAI 本家と Anthropic Messages API 互換へ広げる。pi-ai の 25+ プロバイダ対応をそのまま持つ必要はないが、配管を **OpenAI Chat Completions / OpenAI Responses / Anthropic Messages の3プロトコルアダプター**へ分離し、共通のイベント・メッセージ型へ正規化する必要がある。
5. 開発は AI 駆動であり、pi を設計参照とした忠実なリライトのコストは計画の質に依存し、言語自体には大きく依存しない。

## 決定

**エージェント基盤 (apps/agent) は Rust で実装する。pi はコード流用元ではなく設計参照とし、イベント体系・フック設計・運用細部を忠実に移植する。**

- ランタイム: tokio ベースの長命プロセス。CloudではtenantごとのmicroVM内で動き、ローカル/CIでは同じ境界をDockerで検証する (workspace.md)
- プロバイダ層: OpenAI 互換 Chat Completions、OpenAI Responses、Anthropic Messages 互換の3アダプターを自作し、SSE、ツールコール、reasoning/thinking、usage、終了理由を共通イベントへ正規化する。3つすべてが Cloud release の必須機能であり、共通境界を凍結した後は並行実装できる
- ドメイン操作は `contracts/openapi.yaml` を正典とする契約ファースト原則(言語非依存)の下で apps/api を叩く。現状 `contracts/openapi.yaml` は `/health` 1本のみのため、当面は `apiclient` モジュールに reqwest の薄い手書きクライアントを置き、ドメイン API が3本を超えて配管コストが無視できなくなった時点で、生成クライアント (progenitor 等) 導入を別 ADR で判断する ([実装計画 §2.1・D8](../agent/implementation-plan.md) 参照)

選定理由:

- **常時稼働・長命プロセスとしての堅牢性とメモリ効率**。エージェントはユーザーごとに常駐するため、フットプリントが直接コストに効く
- **単一バイナリ配布**。将来の OSS ローカル版 (ユーザーが自分の API キーで動かす形態) で配布障壁が下がる
- 型システムによる状態機械 (ステア、承認フロー、メモリ層遷移) の表現力
- Founder の意思として長期に抱く心臓部を Rust 資産にする

## 検討した代替案

- **TypeScript + pi (pi-ai / pi-agent-core を直接利用)**: 配管の再発明を回避でき、packages/sdui の zod スキーマを直接共有できる。しかし核となる部分の自作量は同じで、単一バイナリ配布ができず、常駐プロセスの堅牢性・フットプリントで劣後する。zod 共有の利点は SDUI スキーマを JSON Schema として contracts/ 側に置くことで代替する。短期の提供期限を理由とする TS 案は「計画駆動の AI 開発ならリライトは短期で完了する」という判断で退けた。
- **Go (apps/api と言語統一)**: 単一バイナリ配布・低フットプリント・常駐プロセスの堅牢性・チーム既存言語・oapi-codegen 等の契約ツール資産と、選定理由の多くを同様に満たす最有力対抗。退けた理由は2点。(1) ステア・承認フロー・メモリ層遷移・イベント正常形クローズのような状態機械を、enum と所有権で「不正状態を表現不能」にする型表現力が一段劣り、AI 駆動開発でコンパイラを常駐レビュアーとして使う本計画の前提と噛み合わないこと。(2) 心臓部を長期の Rust 資産として抱く Founder の意思。なお API 層 (Go) との言語統一の利点は、契約ファースト (contracts/ が正典) により言語非依存で担保されるため決め手にならなかった。

## 引き受けるトレードオフ

- **pi-ai 相当の配管を自作する**。3プロトコルに面積を限定し、pi のソースと各社の一次 API 仕様を参照して、切り詰め、リトライ、イベント順序、キャッシュ制御、ネイティブ compaction item/block の往復を実装計画に明文化する
- **packages/sdui (zod) を直接 import できない**。エージェントが生成する SDUI カードの検証は JSON Schema 経由で行う
- チームに Rust を書く人間はいない。実装・保守とも AI 駆動が前提であり、それに耐える文書化 (本 ADR、設計文書、実装計画) を維持するコストを引き受ける
