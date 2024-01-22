package kafkahartracemerge

import (
	"context"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/coslks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-az-common/cosmosdb/cosutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/har"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-kafka-har/kafkahartracemerge/internal"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/rs/zerolog/log"
	"sync"
)

const (
	KMContentType = "content-type"
)

type harMergerImpl struct {
	tprod.TransformerProducer
	cfg      *Config
	traceTTL int64
}

func NewConsumer(cfg *Config, wg *sync.WaitGroup) (tprod.TransformerProducer, error) {
	var err error

	ttl := int64(-1)
	if cfg.ProcessorConfig.TraceTTL != 0 {
		ttl = int64(cfg.ProcessorConfig.TraceTTL.Seconds())
	}
	b := harMergerImpl{cfg: cfg, traceTTL: ttl}
	b.TransformerProducer, err = tprod.NewTransformerProducer(cfg.TransformerProducerConfig, wg, &b)
	return &b, err
}

func (b *harMergerImpl) Process(km *kafka.Message, opts ...tprod.TransformerProducerProcessorOption) ([]tprod.Message, tprod.BAMData, error) {
	const semLogContext = "har-trace-merge::process"

	tprodOpts := tprod.TransformerProducerOptions{}
	for _, o := range opts {
		o(&tprodOpts)
	}

	bamData := tprod.BAMData{}

	req, err := newRequestIn(km, tprodOpts.Span)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, bamData, err
	}

	cli, err := coslks.GetCosmosDbContainer("default", b.cfg.ProcessorConfig.CollectionId, false)
	if err != nil {
		log.Error().Err(err).Str("collection-id", b.cfg.ProcessorConfig.CollectionId).Msg(semLogContext + " get cosmos-db container")
		return nil, bamData, err
	}

	storedTrace, err := internal.FindTraceById(context.Background(), cli, req.TraceId)
	if err != nil {
		if err == cosutil.EntityNotFound {
			_, err = internal.InsertTrace(context.Background(), cli, req.TraceId, b.traceTTL, req.Har)
			if err != nil {
				log.Error().Err(err).Str("trace-id", req.TraceId).Msg(semLogContext + " insert trace")
				return nil, bamData, err
			}

			return nil, bamData, nil
		} else {
			log.Error().Err(err).Str("trace-id", req.TraceId).Msg(semLogContext + " entity not found")
			return nil, bamData, err
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
	storedTrace.TTL = b.traceTTL
	storedTrace.StartedDateTime = storedTrace.Trace.Log.FindEarliestStartedDateTime()
	_, err = storedTrace.Replace(context.Background(), cli)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext + " replacing trace")
		return nil, bamData, err
	}

	err = b.persistSelectedTraces(cli, &req)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, bamData, err
	}

	return nil, bamData, nil
}

func (b *harMergerImpl) persistSelectedTraces(cli *azcosmos.ContainerClient, req *RequestIn) error {
	const semLogContext = "har-trace-merge::persist-conspicuous-traces"

	// Should persist the trace.
	if req.HasStatusCode(b.cfg.ProcessorConfig.ShortListedTraces.StatusCodeList) {
		log.Warn().Str("trace-id", req.TraceId).Msg(semLogContext + " marked as prominent")

		ttl := int64(-1)
		if b.cfg.ProcessorConfig.ShortListedTraces.TraceTTL != 0 {
			ttl = int64(b.cfg.ProcessorConfig.ShortListedTraces.TraceTTL)
		}
		_, err := internal.InsertConspicuousTrace(context.Background(), cli, req.TraceId, ttl, req.Har)
		if err != nil {
			log.Error().Err(err).Str("trace-id", req.TraceId).Msg(semLogContext)
		}
	}

	return nil
}

func harEntryCompare(e1, e2 *har.Entry) bool {
	return e1.TraceId < e2.TraceId
}
