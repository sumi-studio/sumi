# Reading Messaging attachments

People can send an attachment through an ordinary Messaging DM. The PA opens
that message and reads its attachment through the existing `open_attachment`
action; no parallel inbox or special diagnostic conversation is required.

The reader returns UTF-8 text for text and JSON attachments, and image content
for supported inline image MIME types. Unsupported formats, including PDF, and
invalid UTF-8 return an ordinary tool error with attachment metadata and an
explicit explanation that the content has not been read. They do not stop the
session. Metadata alone is not successful document reading.

The `opencode-go` Kimi K2.7 Code preset and its alias enable image input. This
matches [Kimi's published model capabilities](https://www.kimi.com/code/docs/en/kimi-code/models.html)
and the [OpenCode model definition](https://github.com/anomalyco/models.dev/blob/dev/providers/opencode-go/models/kimi-k2.7-code.toml).
Explicit `supports_images` configuration still overrides the preset. Changing
only a model ID retains the other preset settings; configure capabilities for
that model as needed.

A regression passes the actual attachment renderer's image result through the
Kimi request serializer and checks decoded bytes and MIME. It also verifies
that disabling image input still omits the image. This establishes request
construction, not live visual understanding. Live attachment acceptance must
separately check an answer available only in the attachment, without placing it
in the message, filename or alternative text.

This does not add PDF extraction, browser rendering or support for every file
format. Existing upload and agent-read size limits remain separate.
