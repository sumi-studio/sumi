-- 0016_attachment_spoiler_and_alt: 送信前に添付へ付ける「ネタバレ」と概要
-- (packet A5 / FEAT-ATT-01).
--
-- どちらも送り手が添付そのものに付ける宣言で、メッセージ本文の性質では
-- ない。ネタバレは受け手の画面で中身を隠す指示、概要は中身を見なくても
-- 何の画像かが分かる代替テキスト。どちらも人間の UI からも
-- PersonalityAgent の読み取り経路からも同じ列を見る。
--
-- 既定は「隠さない・概要なし」。0013 の行はすべてその既定に落ちる。

ALTER TABLE message_attachments
    ADD COLUMN spoiler boolean NOT NULL DEFAULT false,
    -- 空文字は「概要なし」と同じ意味にせず、NOT NULL の空文字で統一する
    -- (filename と同じ扱い: 常に文字列で、無いときは空)。
    ADD COLUMN alt text NOT NULL DEFAULT '' CHECK (length(alt) <= 1000);
