export interface MemoryWorkspaceScope {
  firstSeq: number;
  lastSeq: number;
  source: string;
  candidate: string;
  checks: readonly string[];
}

/** Only the private workspace and finalization protocol are shared by stages.
 * The standing premise of asynchronous organization belongs to SYSTEM. */
export function memoryWorkspaceInstruction(
  scope: MemoryWorkspaceScope,
): string {
  return `## 今回の対象と作業場所

対象は journal seq ${scope.firstSeq}〜${scope.lastSeq} です。対象外の文脈は読み解くために参照できますが、置き換える文章が表すのはこの範囲の経験です。

- 元の文章: ${scope.source}（読み取り専用）
- 草稿: ${scope.candidate}（読み書き可能）

元の文章は、引き継いだ文脈に含まれる対象をそのまま写したものです。今回使える操作は、この2ファイルに対する normal route の file.read と、草稿への file.write です。ツール一覧にある他の操作や、別のファイルの探索は、この作業では使えません。

## 草稿の見直しと確定

草稿を書いたら、元の文章とその版の草稿を最後まで読み、自分の文脈と照らし合わせて見直してください。修正した場合も、修正版を書いた後に両方を読み直します。読み取り結果の has_more が true の場合は、next_offset から続きを読めます。草稿の初回書き込みには expect_version: "none"、更新には現在の version を指定します。

納得できた版を確定するには、ツールを呼ばず、応答本文を次のJSONだけにします。version と sha256 は、読み書き結果にある実際の値を使ってください。
{"action":"review","version":1,"sha256":"草稿のsha256"}

確認が返ったら、その対象版と確認項目を見直します。まだ直すところがあれば草稿を更新して読み直し、review からやり直せます。確定する場合は、別の応答で次のJSONを返します。version・sha256・token は確認結果の値、checks は返された各項目名をキー、確認済みの true を値にしたオブジェクトです。
${JSON.stringify({
  action: "confirm",
  version: 1,
  sha256: "確認対象のsha256",
  token: "確認結果のtoken",
  checks: Object.fromEntries(scope.checks.map((check) => [check, true])),
})}

置き換える文章は元の対象より小さくする必要がありますが、決まった圧縮率はありません。納得できる置き換えにできない場合は、元の文章を最後まで読み、{"action":"review_keep"} で保持の確認を開けます。その確認結果にも同じ confirm 形式で応答してください。草稿を残したまま、今回は対象をそのまま保つ判断ができます。`;
}
