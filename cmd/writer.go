package main

import (
	"errors"
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

func (hpwriter *HyperpilotWriter) Run() {
	timeOut := hpwriter.Server.Config.GetString("writer.timeOut")
	retryTimeout, err := time.ParseDuration(timeOut)
	if err != nil {
		log.Warnf("Parse WriterTimeOut {%s} fail, use default interval 3 min in writer {%s}",
			timeOut, hpwriter.CustomerId)
		retryTimeout = 3 * time.Minute
	}

	batchSize := hpwriter.Server.Config.GetInt("writer.batchSize")
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
			if !hpwriter.Queue.Empty() {
				var batchSamples model.Samples
				for i := 0; i < batchSize; i++ {
					sample := hpwriter.Queue.Dequeue()
					if sample == nil {
						log.Warnf("Writer {%s} get nil sample, because element number inside of queue is less than batch size {%d}",
							hpwriter.CustomerId, batchSize)
						break
					}
					batchSamples = append(batchSamples, sample.(model.Samples)...)
				}

				if len(batchSamples) == 0 {
					continue
				}

				retryWrite := func() error {
					return sendSamples(hpwriter.Writer, batchSamples)
				}

				err := backoff.Retry(retryWrite, b)
				if err != nil {
					log.Warnf("Writer {%s} push sample fail, %d sample are dropped: %s",
						hpwriter.CustomerId, len(batchSamples), err.Error())
				}
				time.Sleep(1 * time.Second)
			}
		}
	}()
}

func (writer *HyperpilotWriter) Put(samples model.Samples) {
	writer.Queue.Enqueue(samples)
}

func sendSamples(w writer, samples model.Samples) error {
	sampleSize := len(samples)
	begin := time.Now()
	err := w.Write(samples)
	duration := time.Since(begin).Seconds()
	sentSamples.WithLabelValues(w.Name()).Add(float64(sampleSize))
	sentBatchDuration.WithLabelValues(w.Name()).Observe(duration)

	log.WithFields(log.Fields{
		"StartTime": begin,
		"Duration":  duration,
	}).Infof("Send %d samples to %s", sampleSize, w.Name())

	if err != nil {
		failedSamples.WithLabelValues(w.Name()).Add(float64(sampleSize))
		return errors.New("Unable to sending samples to remote storage:" + err.Error())
	}

	return nil
}
