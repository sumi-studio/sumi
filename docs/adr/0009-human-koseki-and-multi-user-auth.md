# ADR 0009: 人間の戸籍とマルチユーザー認証

- Status: Accepted
- Date: 2026-08-01
- Amends:
  - [ADR 0008](0008-personality-agent-identity-and-execution-fabric.md)
- Related:
  - [CONTEXT.md](../../CONTEXT.md)
  - [#87](https://github.com/sumi-studio/sumi/issues/87)

## Context

現行のブラウザ認証は `StaticIdentityBindingResolver` による単一 Firebase UID
の暫定バインディング（"deliberately narrow hackathon binding"）であり、
複数のテスター・ユーザーを受け付けられない。Sumi は複数の人間と複数の
人格 agent が同じ Workspace で協働するプロダクトであり（ADR 0008）、
人間側の identity と認可を正式に設計する必要がある。

## Decision

1. **Human も戸籍を持つ。** Human の正本 identity は global に一意な UUIDv7
   の `HumanId` とし、trusted provisioning boundary が一度だけ mint する。
   `PersonalityAgentId` と同型で、tenant／org／Workspace から独立する。
   戸籍は Human（`HumanId`）と人格 agent（`PersonalityAgentId`）の双方が
   登録される global identity 台帳である。
2. **Credential は identity ではない。** ログイン手段（現状は Firebase
   アカウント。暫定で、プロダクション移行時に変わり得る）は戸籍の HumanId
   に紐付ける。1人の Human は複数の Credential を持てる。1つの Credential
   は1人の Human に永久に紐づき、別 Human への付け替えは不可。
3. **初回ログイン = 戸籍への自動登記。** 未バインドの Credential で認証が
   通ったら HumanId を mint し、デフォルトの Secretary
   （`PersonalityAgentId`・鍵・秘密）も同時に登記して Credential を
   紐付ける（self-serve signup）。
4. **Employer が人事権と請求先を持つ。** Employer は Human または
   Workspace/org で、1体の agent につき1時点で1主体。個人サインアップに
   Secretary を同時 mint するのは雇用ポリシーであり、org は別ポリシーを
   取り得る。出張（Employer 不変の一時作業）と異動（Employer 変更）を
   区別し、異動は実装コストが小さいため初期から扱う。agent 本人は金銭を
   受け取らず、第三者は Employer と契約・金銭取引を行う。
5. **direct chat は Employer 本人のみの私信 Surface。** 複数人との会話は
   Discord 等の外部 Surface 自身の共有モデルに委ね、raw direct chat の
   共有機能は作らない。per-human grant は後から足せる構造に留める。
6. **管理者も覗けないをデフォルト契約とする。** 研究・改善目的の
   コンテンツログ取得は研究協力（opt-in）への登録者のみとし、個人の
   サインアップ時に設定へ埋もれさせず正直な文面で依頼する。中身を含ま
   ない運用テレメトリは全対象から常時取得する。abuse 監視の
   break-glass 経路は作らない。
7. **戸籍の正本は control plane の Postgres とする。** 単一ホストは各 agent
   の VM の話であり、サービスホストは multi-user の共有サービスである。
   ローカル開発も含めて Postgres に一本化し、全コンポーネントを Docker
   に載せる（将来の AWS 等への移行を綺麗にする）。開発鯖は WSL 上で
   動かす。
8. **`SUMI_AUTH_FIREBASE_UID` を頂点とする既存の環境契約は置き換える。**
   pre-launch contract replacement を恐れない（ADR 0008 §2 の方針）。

## Consequences

- `StaticIdentityBindingResolver` は戸籍参照の `IdentityBindingResolver`
  実装へ置き換わる。環境変数による単一 UID バインディングは廃止。
- スコープ判断: ハッカソン参加の3名が Sumi Workspace を共同利用するため
  （〆切約21時間）、Workspace/org 実体と覚醒トリガも本 increment と
  並行して実装する。複数の土台を同時に掘るリスクは Founder が承認済み。
- 既知のリスク: 認証・attention・Workspace・コンテナ化は独立に詰まり得る
  土台であり、一箇所の遅延が全体を止めないようストリームを分離して
  進める。
