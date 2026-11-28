package tungo

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"unicode/utf8"
)

// DownloadedFile is the answer of an operation that returns a file (Chat.DownloadFile,
// Proposals.Export, Batches.Export): the bytes, read into memory, with the file name and the
// media type the server sent.
type DownloadedFile struct {
	// Content is the body of the response, byte for byte.
	Content []byte
	// Filename is the name from the Content-Disposition header: the UTF-8 filename* when it is
	// present and valid, otherwise filename; "" when the server sent no name. It is the server's
	// suggestion, not a safe path: do not join it to a directory unchecked.
	Filename string
	// ContentType is the Content-Type header as sent (parameters included); "" when absent.
	ContentType string
	// Response is the HTTP metadata of the call: status, headers, attempts.
	Response *Response
}

// RequestID returns the x-request-id response header: quote it when you contact support.
func (f *DownloadedFile) RequestID() string {
	if f == nil || f.Response == nil {
		return ""
	}
	return f.Response.RequestID()
}

// String describes the file without its bytes.
func (f *DownloadedFile) String() string {
	if f == nil {
		return "<nil>"
	}
	return fmt.Sprintf("DownloadedFile{Filename: %q, ContentType: %q, Size: %d}", f.Filename, f.ContentType, len(f.Content))
}

// download performs a call whose 2xx answer is a file. Errors, retries and timeouts are those of
// every other call.
func (c *Client) download(ctx context.Context, spec *requestSpec, opts []Option) (*DownloadedFile, error) {
	res, err := c.execute(ctx, spec, opts)
	if err != nil {
		return nil, err
	}
	return &DownloadedFile{
		Content:     res.body,
		Filename:    contentDispositionFilename(res.resp.Header.Get("Content-Disposition")),
		ContentType: res.resp.Header.Get("Content-Type"),
		Response:    res.response(),
	}, nil
}

// contentDispositionFilename returns the file name of a Content-Disposition header value: the
// RFC 5987 filename* when its charset is UTF-8 and it decodes to valid UTF-8, otherwise the plain
// filename parameter (a token or a quoted string); "" when there is none.
func contentDispositionFilename(header string) string {
	params := dispositionParams(header)
	if ext, ok := params["filename*"]; ok {
		if charset, rest, ok := strings.Cut(ext, "'"); ok && strings.EqualFold(charset, "utf-8") {
			if _, encoded, ok := strings.Cut(rest, "'"); ok {
				if v, err := url.PathUnescape(encoded); err == nil && utf8.ValidString(v) && v != "" {
					return v
				}
			}
		}
	}
	return params["filename"]
}

// dispositionParams splits the parameters after the disposition type into lower-cased names and
// unquoted values; the first occurrence of a name wins.
func dispositionParams(header string) map[string]string {
	var parts []string
	var cur strings.Builder
	quoted, escaped := false, false
	for _, r := range header {
		switch {
		case escaped:
			escaped = false
		case quoted && r == '\\':
			escaped = true
		case r == '"':
			quoted = !quoted
		case r == ';' && !quoted:
			parts = append(parts, cur.String())
			cur.Reset()
			continue
		}
		cur.WriteRune(r)
	}
	parts = append(parts, cur.String())
	out := map[string]string{}
	for _, p := range parts[1:] {
		name, value, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		name = strings.ToLower(strings.TrimSpace(name))
		value = strings.TrimSpace(value)
		if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
			var b strings.Builder
			esc := false
			for _, r := range value[1 : len(value)-1] {
				if !esc && r == '\\' {
					esc = true
					continue
				}
				esc = false
				b.WriteRune(r)
			}
			value = b.String()
		}
		if _, seen := out[name]; !seen {
			out[name] = value
		}
	}
	return out
}
