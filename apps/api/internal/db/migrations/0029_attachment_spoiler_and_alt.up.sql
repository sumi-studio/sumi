-- 0029_attachment_spoiler_and_alt: 送り手が添付そのものに付ける二つの宣言。
--
-- spoiler は「開くまで中身を見せない」という受け手の画面への指示、alt は
-- 中身を見なくても何のファイルかが分かる説明。どちらもメッセージ本文の
-- 性質ではなく添付の性質なので、message_attachments が持って添付と一緒に
-- 運ぶ。人間の画面も PersonalityAgent の読み取りも同じ列を見る。
--
-- 既定は「隠さない・説明なし」。0023 の既存行はすべてその既定に落ちる。
-- alt は filename と同じく常に文字列で、無いときは NULL ではなく空。
-- 上限は 0017 と同じくバイトで数える（length は文字数で、多バイトの説明が
-- アプリの上限を越えて入り得る）。1000 文字ぶんの余地として 4000 バイト。

ALTER TABLE message_attachments
    ADD COLUMN spoiler boolean NOT NULL DEFAULT false,
    ADD COLUMN alt     text    NOT NULL DEFAULT ''
        CHECK (octet_length(alt) <= 4000);
