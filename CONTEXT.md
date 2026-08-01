# Sumi

Sumi は複数の人間と複数の人格 agent が同じ Workspace で協働するプロダクト。人間と agent を同型の主体として扱う（詳細な ontology は docs/adr/0008 を参照）。

## Language

**Human**:
Sumi を使う人間。1人の Human はデフォルトで1体の **Secretary** を持つ。
_Avoid_: ユーザー（単体で使う場合）、アカウント

**HumanId**:
Human の正本 identity。global に一意な UUIDv7 で、trusted provisioning boundary が一度だけ mint する。PersonalityAgentId と同型で、tenant / org / Workspace から独立する。

**戸籍**:
Sumi の global identity 台帳。Human（HumanId）と人格 agent（PersonalityAgentId）の双方が登録される正本。人間も AI も同じ戸籍を持つ。
_Avoid_: アカウントDB、ユーザーテーブル（identity 台帳の意味では）

**Credential**:
Human が Sumi にログインするための外部認証手段。現状は Firebase アカウント（暫定で、プロダクション移行時に変わり得る）。1人の Human は複数の Credential を戸籍に紐付けられる。1つの Credential は1人の Human に永久に紐づき、別 Human への付け替えは不可。identity ではない。
_Avoid_: アカウント（Human 本人のことか credential のことか曖昧な場合）

**Secretary**:
Human にデフォルトで付き添う人格 agent（PersonalityAgent）。お付きの秘書。デフォルトネームは「Sumi」。
_Avoid_: デフォルトエージェント、アシスタント

**Hire**:
人格 agent を新たに迎え入れること。Human が個人で雇う場合と、org / エンタープライズが Workspace に雇う場合がある。
_Avoid_: プロビジョニング（Human-facing の文脈では）、作成

**Fire**:
役目が終わった人格 agent を解雇すること。Hire の対。
_Avoid_: 削除、デプロビジョニング（Human-facing の文脈では）

**Employer**:
人格 agent の雇用主。人事権と請求先を持つ主体で、Human または Workspace/org がなれる。1体の agent の Employer は1時点で1主体。agent 本人は金銭を受け取らず、第三者が agent の労働の対価を払うときは Employer との間で契約・金銭取引を行う。

**出張**:
agent が Employer の関係を変えずに、別の Workspace で一時的に働くこと。雇用・請求は出張元に残る。個人の Secretary が Workspace で働く場合の基本形。

**異動**:
agent の Employer が変わること。identity と人生ログは連続する（ADR 0008）。実装コストは小さいため初期から扱ってよい。

**Surface**:
agent が人間と接する入口。web の direct chat、mobile、voice、Discord などの外部サービス。raw な direct chat は Employer 本人だけがアクセスできる私信 Surface で、利用は Sumi の拡充とともに低減していく。複数人との会話は各外部 Surface 自身の共有モデルに従う。

**研究協力**:
Human / Employer が研究・改善目的のコンテンツログ取得に明示的に同意すること。デフォルトの人生ログは管理者も覗けない private で、研究協力への登録者のみ解禁される。個人のサインアップ時に、設定に埋もれさせず正直な文面でお願いする。運用テレメトリ（中身を含まないメタデータ）は全対象から常時取得する。

**覚醒トリガ**:
agent session に入力として届き、agent の思考を走らせる出来事。呼びかけ（各 Surface）、予定された出来事、自律的な衝動の3種。attention 設計（issue #87）と一体。自律的な衝動の許可・予算・時間帯の制御は Employer が行う。

**Workspace**:
複数の Human と複数の人格 agent が協働する共有の場。共有の domain data と coordination は API/control plane を通して実装される（ADR 0008）。org/enterprise は Workspace に agent を Hire できる。

**暖気**:
agent のランタイムを応答可能な状態で待機させておくこと。Employer のコスト設定であり、人格の状態ではない。人格 agent に睡眠は存在せず、ランタイムの起停はリソース管理であって人格の生死・睡眠ではない（ADR 0008）。
_Avoid_: スリープ、休眠（人格の性質として使う場合）

## Flagged ambiguities

- **Sumi**: プロダクト名であると同時に、各 Human の Secretary のデフォルトネームでもある。これは意図的な同一視であり、誤りではない。
