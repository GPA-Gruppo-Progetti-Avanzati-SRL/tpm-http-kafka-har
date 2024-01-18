package kafkahartracemerge

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-kafka-common/tprod"
	"time"
)

type Config struct {
	TransformerProducerConfig *tprod.TransformerProducerConfig `yaml:"t-prod,omitempty" mapstructure:"t-prod,omitempty" json:"t-prod,omitempty"`
	ProcessorConfig           *ProcessorConfig                 `yaml:"process,omitempty" mapstructure:"process,omitempty" json:"process,omitempty"`
}

type ProcessorConfig struct {
	CollectionId   string        `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
	TraceTTL       time.Duration `yaml:"trace-ttl,omitempty" mapstructure:"trace-ttl,omitempty" json:"trace-ttl,omitempty"`
	StatusCodeList []int         `yaml:"status-code-filter,omitempty" mapstructure:"status-code-filter,omitempty" json:"status-code-filter,omitempty"`
}
