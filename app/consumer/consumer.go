package consumer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/webdevolegkuprianov/kafka_consumer/model"

	logger "github.com/webdevolegkuprianov/kafka_consumer/app/logger"
)

//reader
func (s Store) reader(ctx context.Context) error {

	errs := make(chan error, 1)

	var data [][]byte
	var byteSize int

	for {

		msg, err := s.consumer.ReadMessage(ctx)
		if err != nil {
			logger.ErrorLogger.Println(err)
			return err
		}

		val := msg.Value

		data = append(data, val)
		byteSize = byteSize + len(val)

		if byteSize > s.config.Spec.Handle.BatchBytes {

			go func() {
				errs <- s.handleData(data)
			}()

			if err := <-errs; err != nil {
				logger.ErrorLogger.Println(err)
			}

			data = nil
			byteSize = 0
		}

	}

}

//handle
func (s Store) handleData(dat [][]byte) error {

	var data [][]interface{}

	for _, k := range dat {

		var r model.Data
		var rs []interface{}

		if err := json.Unmarshal(k, &r); err != nil {
			logger.ErrorLogger.Println(err)
			return err
		}

		rs = append(rs,
			r.Modification,
			r.ModFamily,
			r.ModBodyType,
			r.ModEngine,
			r.ModBase,
			r.UrlMod,
			r.Clientid,
			r.Ymuid,
		)

		data = append(data, rs)

	}

	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFunc()

	tx, err := s.db.Begin(context.Background())
	if err != nil {
		logger.ErrorLogger.Println(err)
		return err
	}

	//orders
	tableData := []string{
		"modification",
		"modfamily",
		"modbodytype",
		"modengine",
		"modbase",
		"urlmod",
		"clientidgoogle",
		"clientid",
	}

	_, err = tx.CopyFrom(ctx, pgx.Identifier{"data"}, tableData, pgx.CopyFromRows(data))
	if err != nil {
		logger.ErrorLogger.Println(err)
		return err
	}

	err = tx.Commit(ctx)
	if err != nil {
		logger.ErrorLogger.Println(err)
		return err
	}

	logger.InfoLogger.Print("db_query :", "ok")

	return nil

}
