# ADR 0006: Artifact textのlossless byte pagination

- Status: Accepted
- Date: 2026-07-22
- Amends: [実装計画](../agent/implementation-plan.md) §8.1〜§8.2、[T13/T15/T21](../../apps/agent/TASKS.md)

## コンテキスト

workspace向け`truncate_head`は50KiB/2000行と完全行表示を守る。一方、同じ規則を
`artifact://`の継続読みに使うと、50KiBを超える単一行は表示0 byteのままcursorを進められない。
またbrokerが返したraw全長でcursorを進めると、2000行上限や注記領域のためrendererが隠したbyteを
飛び越す。完全行、有限envelope、任意長textのlosslessな正進行はこの形では同時に満たせない。

artifactは全文をopaque handleから回収する経路であり、workspace fileの見やすいhead viewとは
目的が異なる。さらにbroker RPCはraw byte pageなので、UTF-8 scalarがpage境界を跨ぐ場合と、
brokerのartifact EOFとmodel-visible page EOFも別々に表現する必要がある。

## 決定

artifact text readだけを専用のpage fragment契約にする。公開`read_file` schemaは変えず、artifact
pathではruntime adapterがRPC前にuser limitを50KiBからworst-case u64 continuation注記とseparatorを
引いた値へcapする。workspace pathはuser limitをそのままexecutorへ渡す。

adapterはbroker rawのUTF-8 scalar境界上の先頭fragmentを直接表示する。logical line途中の開始・終了を
許し、非final pageでは表示sourceをexactな`[request_offset, next_offset)`とする。2000行上限で切る場合も
完全行へ巻き戻さずscalar境界で正進行する。continuationは最大u64表現の容量を予約し、表示source、
separator、注記を合わせて50KiB/2000行以内にする。cursor加算はcheckedとする。

末尾だけが不完全UTF-8でbroker EOFでなければ、`valid_up_to > 0`のprefixだけを表示し、未完scalarを
次pageで先頭から再読する。EOFの不完全文字、interior invalid、continuation byteから始まるoffsetは
fail-closedとする。limitが1 scalarにも足りない場合は同一offsetの空成功を返さずlarger-limit retryを返す。
binaryはlossy decodeせず、将来の明示的binary projectionで扱う。

detailsは`request_offset`、`returned_bytes`、`shown_bytes`、`next_offset`、`artifact_eof`、`page_eof`、
`ends_in_line_fragment`を返す。`page_eof`はbroker EOFかつreturned raw全byteを表示した場合だけtrueで、
その場合だけ`next_offset=null`とする。

## 棄却した代替

- A（読めないartifactを許す、またはgrepだけに限定）: opaque全文をモデルが回収できる製品契約を破る。
- B単独（generic head truncationだけを維持）: 完全行と単一行超過で0-byte loopになり、lossless正進行を満たせない。
- D（brokerをline-awareにする）: presentation上限とcursor責務をstorage境界へ漏らし、runtime注記による容量変化も解決しない。

## 影響と境界

artifact text pageは行単位の見やすさよりbyte-exactな回収可能性を優先し、先頭・末尾がline fragmentに
なる。consumerは`page_eof`と`next_offset`を使い、`artifact_eof`だけで終了判定しない。
workspace read、generic `truncate_head`/`truncate_tail`、bash出力、grepの規則は変えない。
pre-launchのため旧details payloadとの互換層は追加しない。
