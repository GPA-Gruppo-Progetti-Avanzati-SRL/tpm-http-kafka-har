package hartraceconsumer

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"strings"
)

type RequestIn struct {
	Span        opentracing.Span  `yaml:"-" mapstructure:"-" json:"-"`
	ContentType string            `yaml:"content-type,omitempty" mapstructure:"content-type,omitempty" json:"content-type,omitempty"`
	TraceId     string            `yaml:"trace-id,omitempty" mapstructure:"trace-id,omitempty" json:"trace-id,omitempty"`
	Headers     map[string]string `yaml:"headers,omitempty" mapstructure:"headers,omitempty" json:"headers,omitempty"`
	Key         []byte            `yaml:"key,omitempty" mapstructure:"key,omitempty" json:"key,omitempty"`
	Body        []byte            `yaml:"body,omitempty" mapstructure:"body,omitempty" json:"body,omitempty"`
}

func (r *RequestIn) Header(hn string) string {
	if len(r.Headers) > 0 {
		return r.Headers[hn]
	}
	return ""
}

func newRequestIn(km *kafka.Message, span opentracing.Span) (RequestIn, error) {

	const semLogContext = "echo-blob::new-request-in"

	var req RequestIn
	var err error

	headers := make(map[string]string)
	for _, header := range km.Headers {
		headers[header.Key] = string(header.Value)
	}
	req.Headers = headers

	var ct string
	var ok bool
	if ct, ok = req.Headers[KMContentType]; !ok {
		ct = "application/octet-stream"
	} else {
		// remove the semicolon (if present to clean up the content type) to text/xml; charset=utf-8
		ndx := strings.Index(ct, ";")
		if ndx > 0 {
			ct = ct[ndx:]
		}
	}

	req.ContentType = ct

	req.Key = km.Key
	req.Body = km.Value
	req.Span = span
	return req, err
}

func (r *RequestIn) Finish() {
	if r.Span != nil {
		r.Span.Finish()
	}
}
