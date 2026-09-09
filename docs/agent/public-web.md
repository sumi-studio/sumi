# Reading public pages

`public_url_read` lets a PA fetch one public HTTPS document and use its actual
text in the ongoing conversation. A failed fetch returns an ordinary tool error;
it does not claim that the page was read or end the PA's conversation.

The tool accepts `{ "url": "https://example.org/guide#topic" }`. The normal
execution route reviews the complete URL, including its query, before sending
a GET. It sends no account credentials or cookies. A fragment remains in the
requested source reference and is not sent to the server. A redirect returns a
new URL to consider in a separate reviewed call; it is not followed automatically.
The reviewer cannot invoke this tool as an unreviewed read.

## Results and limits

The result includes the requested and fetched URLs, retrieval time, HTTP status,
media type, optional title, extracted text, received-body byte count and SHA-256,
and whether the extracted text was truncated. The hash describes the received
body, not normalized HTML text. The durable tool result retains the observed text
and source; it is not an archive of the complete original HTML.

UTF-8 HTML and plain text are supported. HTML extraction omits script/style and
other embedded content, preserves paragraph boundaries and preformatted code
whitespace, and does not load linked resources. JavaScript-rendered content, PDF,
authenticated pages, other character encodings and compressed responses are not
supported. A page with no readable text is reported as such.

Current bounds are 15 seconds per fetch, 1 MiB received body, 128 KiB returned
text, 1 KiB title, 32 KiB response headers, and four concurrent requests without
a queue. URLs are limited to 8 KiB and HTTPS port 443. The internal client allows
20 seconds for this route; other local-control routes retain their existing
timeout. URLs containing userinfo or altered by the existing secret redactor are
not accepted. DNS hostnames use ASCII form, including punycode where needed.

## Connection boundary

The API authenticates the runtime generation. It resolves the hostname once,
rejects mixed public/nonpublic DNS answers, and connects only to checked literal
addresses while retaining the original TLS hostname and HTTP Host. A fresh
transport does not inherit proxy settings, cookies or authentication. It does
not disable certificate verification.

Destination classification conservatively excludes special-purpose address
blocks, including some globally reachable protocol infrastructure. The source
table cites the IANA registries; classification is not proof of arbitrary host
routing. There is no localhost test exception in the production classifier.

Generation authorization is checked before sending and before returning the
result. Its lease is not held through DNS or network I/O. Cancellation or a later
authority change cannot undo a GET already sent; the code does not claim that it
can. Page content remains tool-sourced material, not a new Human instruction or
permission grant.

## Verification

Go/Rust share URL and response fixtures. Focused tests cover reviewed URL binding,
reviewer restrictions, failed-fetch recovery, authenticated HTTP transport,
IP classification and pinning, redirects, response limits and text extraction.
TLS test connections use a test resolver/dialer seam with local certificates;
production destination rules are not weakened for tests.

Real-model shared acceptance is pending. The prepared opt-in probe reads
[RFC 2606](https://www.rfc-editor.org/rfc/rfc2606.txt), compares its fetched text
with the answer, requests an unavailable `.invalid` name, and verifies subsequent
conversation and reconnect without a new command. Unit-test success alone does
not establish this journey or general browsing capability.
