package consumer

import (
	"context"
	"time"

	logger "github.com/webdevolegkuprianov/kafka_consumer/app/logger"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/log/logrusadapter"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/webdevolegkuprianov/kafka_consumer/model"
)

type Store struct {
	consumer *kafka.Reader
	config   *model.Service
	db       *pgxpool.Pool
}

func Start(config *model.Service) error {

	//conn db
	dbPostgres, err := newDbPostgres(config)
	if err != nil {
		logger.ErrorLogger.Println(err)
		return err
	}

	//init consumer
	consumer := newConsumer(context.Background(), config)

	s := Store{
		consumer: consumer,
		config:   config,
		db:       dbPostgres,
	}

	ctx := context.Background()

	if err := s.reader(ctx); err != nil {
		logger.ErrorLogger.Println(err)
		return err
	}

	return nil
}

func newConsumer(ctx context.Context, config *model.Service) *kafka.Reader {

	kafkaConfig := kafka.ReaderConfig{
		//broker, topics assign
		Brokers: []string{config.Spec.KafkaConf.Brokers.Broker1},
		Topic:   config.Spec.KafkaConf.Topics.Topic1,
		GroupID: config.Spec.KafkaConf.KafkaClientId,
		//consumer settings
		HeartbeatInterval:      time.Duration(config.Spec.ConsumerConf.HeartBeatInterval) * time.Second,
		QueueCapacity:          config.Spec.ConsumerConf.QueueCapacity,
		MinBytes:               config.Spec.ConsumerConf.MinBytes,
		MaxBytes:               config.Spec.ConsumerConf.MaxBytes,
		MaxWait:                time.Duration(config.Spec.ConsumerConf.MaxWait) * time.Second,
		StartOffset:            kafka.LastOffset,
		WatchPartitionChanges:  config.Spec.ConsumerConf.WatchPartitionChanges,
		PartitionWatchInterval: time.Duration(config.Spec.ConsumerConf.PartitionWatchInterval) * time.Second,
		SessionTimeout:         time.Duration(config.Spec.ConsumerConf.SessionTimeout) * time.Second,
		RebalanceTimeout:       time.Duration(config.Spec.ConsumerConf.RebalanceTimeout) * time.Second,
		RetentionTime:          time.Duration(config.Spec.ConsumerConf.RetentionTime) * time.Hour,
		ReadBackoffMin:         time.Duration(config.Spec.ConsumerConf.ReadBackoffMin) * time.Millisecond,
		ReadBackoffMax:         time.Duration(config.Spec.ConsumerConf.ReadBackoffMax) * time.Second,
		Logger:                 logger.KafkaLogger,
	}

	consumer := kafka.NewReader(kafkaConfig)

	return consumer

}

//connect to postgres
func newDbPostgres(conf *model.Service) (*pgxpool.Pool, error) {

	config, _ := pgx.ParseConfig("")
	config.Host = conf.Spec.DB.Host
	config.Port = conf.Spec.DB.Port
	config.User = conf.Spec.DB.User
	config.Password = conf.Spec.DB.Password
	config.Database = conf.Spec.DB.Database
	config.LogLevel = pgx.LogLevelDebug
	config.Logger = logrusadapter.NewLogger(logger.PgLog())
	config.TLSConfig = nil

	poolConfig, _ := pgxpool.ParseConfig("")
	poolConfig.ConnConfig = config
	poolConfig.MaxConnLifetime = time.Duration(conf.Spec.DB.MaxConnLifetime) * time.Minute
	poolConfig.MaxConnIdleTime = time.Duration(conf.Spec.DB.MaxConnIdletime) * time.Minute
	poolConfig.MaxConns = conf.Spec.DB.MaxConns
	poolConfig.MinConns = conf.Spec.DB.MinConns
	poolConfig.HealthCheckPeriod = time.Duration(conf.Spec.DB.HealthCheckPeriod) * time.Minute

	conn, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	if err := conn.Ping(context.Background()); err != nil {
		logger.ErrorLogger.Println(err)
		return nil, err
	}

	return conn, nil
}
