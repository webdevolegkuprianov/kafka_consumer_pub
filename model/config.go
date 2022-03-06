package model

import (
	"io/ioutil"
	"path/filepath"

	logger "github.com/webdevolegkuprianov/kafka_consumer/app/logger"
	"gopkg.in/yaml.v2"
)

//config yaml struct
type Service struct {
	KafkaEcoSystem string `yaml:"kafka_eco_system"`
	Spec           struct {
		KafkaConf struct {
			Brokers struct {
				Broker1 string `yaml:"broker1"`
			} `yaml:"brokers"`
			Topics struct {
				Topic1 string `yaml:"topic1"`
			} `yaml:"topics"`
			Keys struct {
				Key1 string `yaml:"key1"`
			} `yaml:"keys"`
			KafkaClientId string `yaml:"kafka_client_id"`
		} `yaml:"kafka_conf"`
		ConsumerConf struct {
			HeartBeatInterval      int  `yaml:"heart_beat_interval"`
			QueueCapacity          int  `yaml:"queue_capacity"`
			MinBytes               int  `yaml:"min_bytes"`
			MaxBytes               int  `yaml:"max_bytes"`
			MaxWait                int  `yaml:"max_wait"`
			WatchPartitionChanges  bool `yaml:"watch_partition_changes"`
			PartitionWatchInterval int  `yaml:"partition_watch_interval"`
			SessionTimeout         int  `yaml:"session_timeout"`
			RebalanceTimeout       int  `yaml:"rebalance_timeout"`
			RetentionTime          int  `yaml:"retention_time"`
			ReadBackoffMin         int  `yaml:"read_backoff_min"`
			ReadBackoffMax         int  `yaml:"read_backoff_max"`
		} `yaml:"consumer"`
		Handle struct {
			BatchSize  int `yaml:"batch_size"`
			BatchBytes int `yaml:"batch_bytes"`
		} `yaml:"handle"`
		DB struct {
			Name              string `yaml:"name"`
			Host              string `yaml:"host"`
			Port              uint16 `yaml:"port"`
			User              string `yaml:"user"`
			Password          string `yaml:"password"`
			Database          string `yaml:"database"`
			MaxConnLifetime   int    `yaml:"max_conn_lifetime"`
			MaxConnIdletime   int    `yaml:"max_conn_idletime"`
			MaxConns          int32  `yaml:"max_conns"`
			MinConns          int32  `yaml:"min_conns"`
			HealthCheckPeriod int    `yaml:"health_check_period"`
		} `yaml:"db"`
		Logs struct {
			Path string `yaml:"path"`
		} `yaml:"logs"`
	} `yaml:"spec"`
}

//new config
func NewConfig() (*Service, error) {

	var service *Service

	f, err := filepath.Abs("/root/config/kafka.yaml")
	if err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	y, err := ioutil.ReadFile(f)
	if err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	if err := yaml.Unmarshal(y, &service); err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	return service, nil

}
