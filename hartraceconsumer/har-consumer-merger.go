package hartraceconsumer

import (
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

type echoImpl struct {
	tprod.TransformerProducer
	cfg *Config
}

func NewConsumer(cfg *Config, wg *sync.WaitGroup) (tprod.TransformerProducer, error) {
	var err error
	b := echoImpl{cfg: cfg}
	b.TransformerProducer, err = tprod.NewTransformerProducer(cfg.TransformerProducerConfig, wg, &b)
	return &b, err
}

func (b *echoImpl) Process(km *kafka.Message, span opentracing.Span) (tprod.Message, tprod.BAMData, error) {
	const semLogContext = "har-trace-consumer::process"

	bamData := tprod.BAMData{}
	bamData.AddLabel("test_label", "test_value")

	req, err := newRequestIn(km, span)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return tprod.Message{}, bamData, err
	}

	return tprod.Message{
		Span:      req.Span,
		TopicType: "std",
		Headers:   req.Headers,
		Key:       req.Key,
		Body:      req.Body,
	}, bamData, nil
}
