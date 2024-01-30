package kafkahartracer

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-http-archive/hartracing/factory"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/kafkalks"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"strings"
)

const (
	HarKafkaTracerType    = "har-kafka-tracer"
	BrokerNameEnvVar      = "HAR_KAFKA_BROKER_NAME"
	TopicNameEnvVar       = "HAR_KAFKA_TOPIC_NAME"
	MetricGroupIdEnvVar   = "HAR_KAFKA_METRIC_GROUP_ID"
	MetricCounterIdEnvVar = "HAR_KAFKA_METRIC_COUNTER_ID"
)

func IsHarTracerTypeFromEnvSupported() bool {
	const semLogContext = "har-tracing::is-type-from-env-supported"
	trcType := factory.HarTracerTypeFromEnv()
	if trcType == "" {
		log.Info().Msgf(semLogContext+" env var %s not set", hartracing.HARTracerTypeEnvName)
	}

	return trcType == HarKafkaTracerType
}

func InitHarTracingFromEnv() (io.Closer, error) {

	const semLogContext = "kafka-har-tracing::init-from-env"
	const semLogLabelTracerType = "tracer-type"

	var trc hartracing.Tracer
	var closer io.Closer
	var err error

	trcType := factory.HarTracerTypeFromEnv()
	if trcType == "" {
		return closer, nil
	}

	log.Info().Str(semLogLabelTracerType, trcType).Msg(semLogContext)
	switch strings.ToLower(trcType) {
	case HarKafkaTracerType:
		brokerName := os.Getenv(BrokerNameEnvVar)
		if brokerName == "" {
			err = fmt.Errorf("broker name environment variable %s not set", BrokerNameEnvVar)
			log.Error().Err(err).Str(semLogLabelTracerType, trcType).Msgf(semLogContext)
			return nil, err
		}

		topic := os.Getenv(TopicNameEnvVar)
		if topic == "" {
			err = fmt.Errorf("topic name environment variable %s not set", TopicNameEnvVar)
			log.Error().Err(err).Str(semLogLabelTracerType, trcType).Msgf(semLogContext)
			return nil, err
		}

		lks, err := kafkalks.GetKafkaLinkedService(brokerName)
		if err != nil {
			return nil, err
		}

		metricRef := promutil.MetricsConfigReference{
			GId:         os.Getenv(MetricGroupIdEnvVar),
			CounterId:   os.Getenv(MetricCounterIdEnvVar),
			HistogramId: "-",
			GaugeId:     "-",
		}
		if metricRef.GId == "" {
			metricRef.GId = "-"
		}
		if metricRef.CounterId == "" {
			metricRef.CounterId = "-"
		}
		trc, closer, err = NewTracer(
			WithKafkaLinkedService(lks),
			WithTopic(topic),
			WithMetricsConfigReference(&metricRef))
		if err != nil {
			return nil, err
		}

	default:
		log.Info().Str(semLogLabelTracerType, trcType).Msg(semLogContext + " unrecognized tracer type")
	}

	if trc != nil {
		hartracing.SetGlobalTracer(trc)
	}

	return closer, nil
}
