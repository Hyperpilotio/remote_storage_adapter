package main

import (
	"time"

	"github.com/cenkalti/backoff"
	"github.com/hyperpilotio/remote_storage_adapter/pkg/common/queue"
	"github.com/prometheus/common/model"
	log "github.com/sirupsen/logrus"
)

type HyperpilotWriter struct {
	Queue      *queue.Queue
	Writer     writer
	CustomerId string
	Server     *Server
}

func NewHyperpilotWriter(server *Server, w writer, customerId string) (*HyperpilotWriter, error) {
	queueSize := server.Config.GetInt("writer.queueSize")
	return &HyperpilotWriter{
		Queue:      queue.NewCappedQueue(queueSize),
		Writer:     w,
		CustomerId: customerId,
		Server:     server,
	}, nil
}

func (writer *HyperpilotWriter) Run() {
	timeOut := writer.Server.Config.GetString("writer.timeOut")
	retryTimeout, err := time.ParseDuration(timeOut)
	if err != nil {
		log.Warnf("Parse WriterTimeOut {%s} fail, use default interval 3 min in writer {%s}",
			timeOut, writer.CustomerId)
		retryTimeout = 3 * time.Minute
	}

	batchSize := writer.Server.Config.GetInt("writer.batchSize")
	if batchSize < 1 {
		log.Warnf("Batch Size {%d} is not feasible, use 1 instead", batchSize)
		batchSize = 1
	}

	go func() {
		b := backoff.NewExponentialBackOff()
		b.InitialInterval = 10 * time.Second
		b.MaxInterval = 1 * time.Minute
		b.MaxElapsedTime = retryTimeout

		for {
			if !writer.Queue.Empty() {
				var batchSamples model.Samples
				for i := 0; i < batchSize; i++ {
					sample := writer.Queue.Dequeue()
					if sample == nil {
						log.Warnf("Writer {%s} get nil sample, because element number inside of queue is less than batch size {%d}",
							writer.CustomerId, batchSize)
						continue
					}
					batchSamples = append(batchSamples, sample.(model.Samples)...)
				}

				retryWrite := func() error {
					writerName := writer.Writer.Name()
					begin := time.Now()
					err := writer.Writer.Write(batchSamples)
					duration := time.Since(begin).Seconds()
					if err != nil {
						failedSamples.WithLabelValues(writerName).Add(float64(len(batchSamples)))
					}
					sentSamples.WithLabelValues(writerName).Add(float64(len(batchSamples)))
					sentBatchDuration.WithLabelValues(writerName).Observe(duration)
					return err
				}

				err := backoff.Retry(retryWrite, b)
				if err != nil {
					log.Warnf("Writer {%s} push sample fail, %d sample are dropped: %s", writer.CustomerId, len(batchSamples), err.Error())
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (writer *HyperpilotWriter) Put(samples model.Samples) {
	writer.Queue.Enqueue(samples)
}
