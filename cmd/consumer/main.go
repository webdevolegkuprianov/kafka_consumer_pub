package main

import (
	"github.com/webdevolegkuprianov/kafka_consumer/app/consumer"
	logger "github.com/webdevolegkuprianov/kafka_consumer/app/logger"
	"github.com/webdevolegkuprianov/kafka_consumer/model"
)

func main() {

	config, err := model.NewConfig()
	if err != nil {
		logger.ErrorLogger.Println(err)
	}

	if err := consumer.Start(config); err != nil {
		logger.ErrorLogger.Println(err)
	}

}
