package logger

import (
	"log"
	"os"

	"github.com/sirupsen/logrus"
)

type errorLog struct {
}
type errorLogPg struct {
}

func (e errorLog) Write(p []byte) (n int, err error) {

	f, err := os.OpenFile("/root/kafka_client/logs.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	f.WriteString(string(p))

	defer f.Close()

	return n, err
}

func (e errorLogPg) Write(p []byte) (n int, err error) {

	f, err := os.OpenFile("/root/kafka_client/logs_pg.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	f.WriteString(string(p))

	defer f.Close()

	return n, err
}

var (
	InfoLogger    = log.New(errorLog{}, "INFO_Consumer: ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmicroseconds)
	ErrorLogger   = log.New(errorLog{}, "ERROR_Consumer: ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmicroseconds)
	WarningLogger = log.New(errorLog{}, "WARNING_Consumer: ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmicroseconds)
	KafkaLogger   = log.New(errorLog{}, "KAFKA_consumer: ", log.Ldate|log.Ltime|log.Lshortfile|log.Lmicroseconds)
)

func PgLog() *logrus.Logger {

	l := logrus.New()
	l.Out = errorLogPg{}
	l.Formatter = new(logrus.JSONFormatter)
	l.Level = logrus.InfoLevel
	l.ExitFunc = os.Exit
	l.ReportCaller = false

	return l
}
