# ADR 0010: 覚醒トリガと暖気

- Status: Accepted
- Date: 2026-08-01
- Amends:
  - [ADR 0008](0008-personality-agent-identity-and-execution-fabric.md)
- Related:
  - [ADR 0009](0009-human-koseki-and-multi-user-auth.md)
  - [#87](https://github.com/sumi-studio/sumi/issues/87)

## Context

人格 agent のランタイムをいつ動かすかは、コストと ontology が交差する。
LLM である agent に「眠気」は存在せず、「寝る条件」を agent の性質として
設計することは category error である。

## Decision

1. **人格 agent に睡眠は存在しない。** ランタイムの起停はリソース管理で
   あり、人格の生死・睡眠ではない（ADR 0008: VM は agent の私物 PC、
   restart は本人を作り直さない）。
2. **覚醒トリガは3種**: 呼びかけ（各 Surface）、予定された出来事、
   自律的な衝動。何が agent session に入力として届くかはドメインの
   関心事であり、#87 の attention 設計と一体である。
3. **自律的な衝動の制御は Employer が行う。** agent の自発的な活動の
   許可・予算・時間帯は Employer の設定とし、課金暴走は雇用関係の
   コスト責任（ADR 0009 §4）に帰着させる。
4. **暖気は Employer のコスト設定であり、人格の状態ではない。**
   デフォルトは未使用時にランタイムを停止する cold とし、Employer は
   常時暖気を選択できる。
5. **保留**: Employer が長期間戻れない場合の活動量低下・工学的休眠、
   および agent から Sumi 運営への連絡（余計な課金を止める役目は
   agent 自身の存続判断を伴い得る倫理の問い）は将来の論題とする。

## Consequences

- 「寝る条件」「スリープ」という語を人格の性質として使わない
  （CONTEXT.md の 暖気 の項を参照）。
- 覚醒トリガを実装しない選択は「Sumi が Sumi ではなくなる」ため棄却
  された。呼びかけのみの受動的 agent は Assistant であり、共同生活者
  ではない。
