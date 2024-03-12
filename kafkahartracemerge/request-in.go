package kafkahartracemerge

import (
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod"
	"strings"
)

type RequestIn struct {
	msg         tprod.Message
	ContentType string   `yaml:"content-type,omitempty" mapstructure:"content-type,omitempty" json:"content-type,omitempty"`
	TraceId     string   `yaml:"trace-id,omitempty" mapstructure:"trace-id,omitempty" json:"trace-id,omitempty"`
	Har         *har.HAR `yaml:"har,omitempty" mapstructure:"har,omitempty" json:"har,omitempty"`
}

func (r *RequestIn) Header(hn string) string {
	if len(r.msg.Headers) > 0 {
		return r.msg.Headers[hn]
	}
	return ""
}

func (r *RequestIn) HasStatusCode(sts []int) bool {

	for _, e := range r.Har.Log.Entries {
		if e.Response != nil {
			for _, st := range sts {
				if st == e.Response.Status {
					return true
				}
			}
		}
	}
	return false
}

func newRequestIn(m tprod.Message) (RequestIn, error) {

	const semLogContext = "echo-blob::new-request-in"

	req := RequestIn{msg: m}
	var err error

	req.ContentType = "application/octet-stream"
	if hd, ok := m.Headers[hartracing.HARTraceIdHeaderName]; !ok {
		req.TraceId = hd
	}

	var ct string
	var ok bool
	if ct, ok = m.Headers[KMContentType]; !ok {
		ct = "application/octet-stream"
	} else {
		// remove the semicolon (if present to clean up the content type) to text/xml; charset=utf-8
		ndx := strings.Index(ct, ";")
		if ndx > 0 {
			ct = ct[ndx:]
		}
	}
	req.ContentType = ct

	/*
		headers := make(map[string]string)
		for _, header := range km.Headers {
			headers[header.Key] = string(header.Value)
			switch header.Key {
			case KMContentType:
				req.ContentType = string(header.Value)
				ndx := strings.Index(string(header.Value), ";")
				if ndx > 0 {
					req.ContentType = req.ContentType[ndx:]
				}
			case hartracing.HARTraceIdHeaderName:
				req.TraceId = string(header.Value)
			}
		}
		req.Headers = headers
	*/

	var harLog har.HAR
	err = json.Unmarshal(m.Body, &harLog)
	if err != nil {
		return req, err
	}

	req.Har = &harLog

	if req.TraceId == "" {
		err = errors.New("har-trac-id missing from message")
	}
	return req, err
}

/*func (r *RequestIn) Finish() {
	if r.msg.Span != nil {
		r.msg.Span.Finish()
	}
}
*/
