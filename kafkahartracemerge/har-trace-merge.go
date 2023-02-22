package kafkahartracemerge

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-kafka-har/kafkahartracemerge/internal"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"sync"
)

const (
	KMContentType = "content-type"
)

type Config struct {
	TransformerProducerConfig *tprod.TransformerProducerConfig `yaml:"t-prod,omitempty" mapstructure:"t-prod,omitempty" json:"t-prod,omitempty"`
	ProcessorConfig           *ProcessorConfig                 `yaml:"process,omitempty" mapstructure:"process,omitempty" json:"process,omitempty"`
}

type ProcessorConfig struct {
	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
}

type harMergerImpl struct {
	tprod.TransformerProducer
	cfg *Config
}

func NewConsumer(cfg *Config, wg *sync.WaitGroup) (tprod.TransformerProducer, error) {
	var err error
	b := harMergerImpl{cfg: cfg}
	b.TransformerProducer, err = tprod.NewTransformerProducer(cfg.TransformerProducerConfig, wg, &b)
	return &b, err
}

func (b *harMergerImpl) Process(km *kafka.Message, span opentracing.Span) (tprod.Message, tprod.BAMData, error) {
	const semLogContext = "har-trace-merge::process"

	bamData := tprod.BAMData{}

	req, err := newRequestIn(km, span)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return tprod.Message{}, bamData, err
	}

	cli, err := coslks.GetCosmosDbContainer("default", b.cfg.ProcessorConfig.CollectionId, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return tprod.Message{}, bamData, err
	}

	storedTrace, err := internal.FindTraceById(context.Background(), cli, req.TraceId)
	if err != nil {
		if err == cosutil.EntityNotFound {
			_, err = internal.InsertTrace(context.Background(), cli, req.TraceId, req.Har)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return tprod.Message{}, bamData, err
			}
		} else {
			log.Error().Err(err).Msg(semLogContext)
			return tprod.Message{}, bamData, err
		}
	}

	var mergeResult *har.HAR
	if req.Har.Log.TraceId < storedTrace.Trace.Log.TraceId {
		log.Trace().Str("into-log-id", req.Har.Log.TraceId).Str("from-log-id", storedTrace.Trace.Log.TraceId).Msg(semLogContext + " add file log to current log")
		mergeResult, err = req.Har.Merge(storedTrace.Trace, harEntryCompare)
	} else {
		log.Trace().Str("from-log-id", req.Har.Log.TraceId).Str("into-log-id", storedTrace.Trace.Log.TraceId).Msg(semLogContext + " add current log to file log")
		mergeResult, err = storedTrace.Trace.Merge(req.Har, harEntryCompare)
	}
	storedTrace.Trace = mergeResult
	_, err = storedTrace.Replace(context.Background(), cli)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return tprod.Message{}, bamData, err
	}

	return tprod.Message{}, bamData, nil
}

func harEntryCompare(e1, e2 *har.Entry) bool {
	return e1.TraceId < e2.TraceId
}
