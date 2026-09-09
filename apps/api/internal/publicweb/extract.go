package publicweb

import (
	"bytes"
	"context"
	"io"
	"mime"
	"strings"
	"unicode"
	"unicode/utf8"

	"golang.org/x/net/html"
)

type textBuffer struct {
	value                     strings.Builder
	limit                     int
	truncated, space, newline bool
}

func (b *textBuffer) write(raw string) {
	if b.truncated {
		return
	}
	for _, r := range raw {
		if unicode.IsSpace(r) {
			b.space = true
			continue
		}
		prefix := ""
		if b.value.Len() > 0 {
			if b.newline {
				prefix = "\n"
			} else if b.space {
				prefix = " "
			}
		}
		if b.value.Len()+len(prefix)+utf8.RuneLen(r) > b.limit {
			b.truncated = true
			return
		}
		b.value.WriteString(prefix)
		b.value.WriteRune(r)
		b.space = false
		b.newline = false
	}
}
func (b *textBuffer) writePre(raw string) {
	if b.truncated {
		return
	}
	if b.newline && b.value.Len() > 0 && !strings.HasPrefix(raw, "\n") {
		raw = "\n" + raw
	}
	b.space = false
	b.newline = false
	remaining := b.limit - b.value.Len()
	if len(raw) > remaining {
		end := remaining
		for end > 0 && !utf8.RuneStart(raw[end]) {
			end--
		}
		raw = raw[:end]
		b.truncated = true
	}
	b.value.WriteString(raw)
}
func (b *textBuffer) boundary() { b.newline = true }
func extract(ctx context.Context, body []byte, media string) (string, *string, bool, error) {
	if media == "text/plain" {
		if len(body) <= MaxTextBytes {
			return string(body), nil, false, nil
		}
		end := MaxTextBytes
		for end > 0 && !utf8.RuneStart(body[end]) {
			end--
		}
		return string(body[:end]), nil, true, nil
	}
	text := textBuffer{limit: MaxTextBytes}
	title := textBuffer{limit: maxTitleBytes}
	tokenizer := html.NewTokenizer(bytes.NewReader(body))
	ignored := ""
	ignoredDepth := 0
	inHead, inTitle, inPre := false, false, false
	for {
		if ctx.Err() != nil {
			return "", nil, false, networkFailure(ctx, ctx.Err(), "fetch_failed")
		}
		kind := tokenizer.Next()
		if kind == html.ErrorToken {
			if tokenizer.Err() != io.EOF {
				return "", nil, false, fail("unsupported_content")
			}
			break
		}
		token := tokenizer.Token()
		switch kind {
		case html.StartTagToken, html.SelfClosingTagToken:
			tag := token.Data
			if ignored != "" {
				if tag == ignored && kind == html.StartTagToken {
					ignoredDepth++
				}
				continue
			}
			switch tag {
			case "script", "style", "template", "noscript", "svg", "canvas", "iframe", "object":
				if kind == html.StartTagToken {
					ignored = tag
					ignoredDepth = 1
				}
				continue
			}
			if tag == "meta" {
				content, httpEquiv := "", ""
				for _, attribute := range token.Attr {
					if attribute.Key == "charset" && !strings.EqualFold(attribute.Val, "utf-8") {
						return "", nil, false, fail("unsupported_content")
					}
					if attribute.Key == "http-equiv" {
						httpEquiv = attribute.Val
					}
					if attribute.Key == "content" {
						content = attribute.Val
					}
				}
				if strings.EqualFold(httpEquiv, "content-type") {
					_, params, e := mime.ParseMediaType(content)
					if e == nil && params["charset"] != "" && !strings.EqualFold(params["charset"], "utf-8") {
						return "", nil, false, fail("unsupported_content")
					}
				}
			}
			if tag == "pre" {
				inPre = true
			}
			if tag == "br" && inPre {
				text.writePre("\n")
			}
			if tag == "head" {
				inHead = true
			}
			if tag == "body" {
				inHead = false
			}
			if tag == "title" {
				inTitle = true
			}
			if blockTag(tag) {
				text.boundary()
			}
		case html.EndTagToken:
			if ignored != "" {
				if token.Data == ignored {
					ignoredDepth--
					if ignoredDepth == 0 {
						ignored = ""
					}
				}
				continue
			}
			if token.Data == "pre" {
				inPre = false
			}
			if token.Data == "head" {
				inHead = false
			}
			if token.Data == "title" {
				inTitle = false
			}
			if blockTag(token.Data) {
				text.boundary()
			}
		case html.TextToken:
			if ignored != "" {
				continue
			}
			if inTitle {
				title.write(token.Data)
			} else if !inHead {
				if inPre {
					text.writePre(token.Data)
				} else {
					text.write(token.Data)
				}
			}
		}
	}
	var heading *string
	if title.value.Len() > 0 {
		value := title.value.String()
		heading = &value
	}
	return text.value.String(), heading, text.truncated, nil
}
func blockTag(tag string) bool {
	switch tag {
	case "address", "article", "aside", "blockquote", "br", "div", "dl", "dt", "dd", "fieldset", "figcaption", "figure", "footer", "form", "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "li", "main", "nav", "ol", "p", "pre", "section", "table", "td", "th", "tr", "ul":
		return true
	}
	return false
}
