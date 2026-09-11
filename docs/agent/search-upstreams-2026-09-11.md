# SearXNG 接続先の比較 — 2026-09-11

SumiのWeb検索に使う接続先について、公式のAPI・利用条件・料金を比較した。
これは公開条件の確認であり、個別契約の法的判断や検索品質の実測ではない。
登録、課金、問い合わせ、SearXNGのインストールは行っていない。

## 現時点の判断

自前SearXNGは接続先を選んで結果をまとめる仕組みであり、検索先の利用条件や
ブロックをなくすものではない。今回確認した範囲では、無料・登録不要・日本語の
汎用検索・製品内での表示・結果の継続保存をすべて満たす接続先は確認できなかった。
未確認と禁止は区別する。個人の試用と、複数人が利用するSumiへの組込みも同一視しない。

Sumiでは検索結果がツール出力として会話履歴に残り得るため、料金と同じくらい
保存条件が重要になる。安い検索プランへ合わせて記憶を壊す設計にはしない。

| 接続先 | 費用・入口 | 利用条件と候補としての位置づけ |
| --- | --- | --- |
| Mojeek公式API | Startup £2/1,000回、Business £3/1,000回、税別。申込みが必要 | AI利用、他の検索結果との混合を明示的に認める。Startupは1時間キャッシュのみ、Businessは保存可。条件面の比較基準にできる有料候補。日本語品質は未測定。 |
| Brave公式API | $5/1,000回、毎月$5クレジット。アカウント・カード・キーが必要 | SearXNGには公式API用braveapi接続もある。ただし現在のFAQはAPI結果の保持を禁止し、保存権は個別相談としている。標準プランをそのまま永続履歴へ接続できるとは扱わない。 |
| Marginalia | 実験用共有publicキー、無料非商用キー、商用キー | 無料結果はCC-BY-NC-SA、共有キーは共有制限あり。補助的な探索の試験候補。日本語の主検索として薦める実測根拠はない。 |
| Mwmbl | 基本検索は登録不要、SearXNG接続あり | データセットはCC-BY-NC-SA。規約にはスクレイピング・過剰な自動照会の制限。コードがオープンでも商用結果利用が無条件に自由ではない。 |
| Google | Web画面用接続はキー不要。公式CSEは新規受付終了 | /searchの自動取得は公式の機械可読指示・利用条件の検討が必要。CSEは2027-01-01終了予定で、新規導入の解決策にはならない。 |
| Bing | Web画面用接続あり | 旧Bing Search APIは2025-08-11終了。案内されるAzure AIのgroundingは同じ生の検索APIではない。 |
| DuckDuckGo | Web画面用接続はキー不要 | AUPに他サービス内でのサービス部分の表示・再販売を制限する記載がある。SearXNG用実装があることから製品利用の許諾までは導けない。 |
| Qwant | SearXNGは非公開APIを利用 | 明示の合意なしにQwant以外でのAPI利用権を付与しない旨が規約にある。標準の製品接続先には選ばない。 |
| Startpage | Web画面用接続はキー不要 | 公式は無料スクレイピングを抑制する理由を説明している。第三者の製品向け検索APIとしての許諾は今回確認できなかった。 |

一つのAPIだけ使うなら、まず直接接続した方が保守箇所は少ない。複数の接続先が
有用だと確認できてからSearXNGを挟む価値を判断する。これは構成案であり、採用決定ではない。

## 品質についてまだ分かっていないこと

日本語の関連性、最新情報、地域情報、技術文書の発見率、応答時間、空振り・ブロック率は
未測定。コネクターの人気や検索エンジンの知名度を実測の代わりにしない。
試す際は同じ公開情報の質問セットで接続先ごとの結果を比較し、保存条件も守る。
集約後の成績だけで、どの接続先が役立ったかを見失わないようにする。

先行調査では匿名Exa MCPとChatGPTログイン済みCodexの検索経路が各1回ずつ動作した。
これは利用可能性の確認に限り、長期安定性・日本語品質・製品提供条件の証明ではない。
本比較によって有料APIの契約や自動課金を開始してはいない。

## 一次資料

- [SearXNG 接続先一覧](https://docs.searxng.org/user/configured_engines.html)
- [SearXNG Brave / braveapi](https://docs.searxng.org/dev/engines/online/brave.html)
- [Mojeek 料金・保存・AI利用FAQ](https://www.mojeek.com/services/search/web-search-api/)
- [Brave料金](https://brave.com/search/api/)、[保持条件・カード要件](https://api-dashboard.search.brave.com/documentation/resources/help-feedback)、[API利用規約](https://api-dashboard.search.brave.com/documentation/resources/terms-of-service)
- [Marginalia API条件](https://about.marginalia-search.com/article/api/)
- [Mwmbl規約](https://mwmbl.org/terms)
- [Google CSE](https://developers.google.com/custom-search/v1/overview)、[Google規約](https://policies.google.com/terms?hl=en-US)、[robots.txt](https://www.google.com/robots.txt)
- [Bing API終了](https://learn.microsoft.com/en-us/lifecycle/announcements/bing-search-api-retirement)
- [DuckDuckGo AUP](https://duckduckgo.com/acceptable-use)
- [Qwant規約](https://about.qwant.com/en/legal/qwant-search/)、[SearXNG Qwant実装説明](https://docs.searxng.org/dev/engines/online/qwant.html)
- [Startpage スクレイピング対策の説明](https://support.startpage.com/hc/en-us/articles/4455380450836-How-does-Startpage-prevent-scraping-and-abuse-without-recording-IP-addresses)
