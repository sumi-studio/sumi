-- 0021_voice_channels: チャンネルを「話す場所」にできるようにする (ADR 0012)
--
-- ボイスチャンネルは別種の place ではなく、channel の一属性である。テキストの
-- timeline も未読も mention も通知設定も、これまでどおり同じ列に乗る——
-- ボイスチャンネルでも文字で会話が続けられることが要件だからである。
-- 別テーブルや別 kind にすると、その全部を二重に持つことになる。
--
-- 通話そのものの状態（今誰が入っているか）はここに持たない。volatile な
-- 事実であり、正本は api のメモリと LiveKit の webhook にある（ADR 0012）。
ALTER TABLE places ADD COLUMN voice boolean NOT NULL DEFAULT false;

-- dm/group_dm も通話はできるが、それは「その場で始める通話」であって、
-- 常設の話す場所ではない。voice が立つのは channel だけに限る。
ALTER TABLE places ADD CONSTRAINT places_voice_is_channel_only
    CHECK (NOT voice OR kind = 'channel');
