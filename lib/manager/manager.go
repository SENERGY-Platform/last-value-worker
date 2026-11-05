/*
 * Copyright 2022 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package manager

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	"github.com/SENERGY-Platform/last-value-worker/lib/consumer"
	kafkaAdmin "github.com/SENERGY-Platform/last-value-worker/lib/kafka-admin"
	"github.com/SENERGY-Platform/last-value-worker/lib/log"
	"github.com/SENERGY-Platform/last-value-worker/lib/memcached"
	"github.com/SENERGY-Platform/last-value-worker/lib/meta"
	"github.com/SENERGY-Platform/last-value-worker/lib/psql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Manager struct {
	config             *config.Config
	consumer           *consumer.Consumer
	deviceTypeConsumer *consumer.Consumer
	consumerCancel     context.CancelFunc
	consumerMux        sync.Mutex
	parentContext      context.Context
	wg                 *sync.WaitGroup
	kafkaAdm           *kafkaAdmin.KafkaAdmin
	memcached          *memcached.Memcached
	psqlPublisher      *psql.Publisher
}

var writtenCounter = promauto.NewCounter(prometheus.CounterOpts{
	Name: "last_value_worker_written",
	Help: "Number of values the worker has written",
})

var writtenCounterDevices = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "last_value_worker_written_device_messages",
	Help: "Number of device messages the worker has written",
},
	[]string{"device_id"})

var writtenBytesCounterDevices = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "last_value_worker_written_device_bytes",
	Help: "Size of device messages the worker has written",
},
	[]string{"device_id"})

func New(c *config.Config, ctx context.Context, wg *sync.WaitGroup) (*Manager, error) {
	if c == nil || ctx == nil {
		return nil, errors.New("nil argument")
	}

	kafkaAdm, err := kafkaAdmin.New(c)
	if err != nil {
		return nil, err
	}
	mc, err := memcached.New(c)
	if err != nil {
		return nil, err
	}
	psqlPublisher, err := psql.New(c.PostgresHost, c.PostgresPort, c.PostgresUser, c.PostgresPw, c.PostgresDb, c.Debug, mc, wg, ctx)
	if err != nil {
		return nil, err
	}
	wd := &Manager{config: c, parentContext: ctx, kafkaAdm: kafkaAdm, wg: wg, memcached: mc, psqlPublisher: psqlPublisher}
	err = wd.start()
	if err != nil {
		return nil, err
	}
	return wd, nil
}

func (wd *Manager) start() error {
	refreshInterval, err := time.ParseDuration(wd.config.TopicRefreshInterval)
	if err != nil {
		return fmt.Errorf("invalid topic_refresh_interval: %w", err)
	}
	if refreshInterval <= 0 {
		return errors.New("topic_refresh_interval must be positive")
	}
	err = wd.newConsumer()
	if err != nil {
		return err
	}
	wd.deviceTypeConsumer, err = consumer.NewConsumer(wd.parentContext, wd.wg, wd.config.KafkaBootstrap, []string{wd.config.DeviceTypeTopic},
		wd.config.DeviceTypeGroupId, consumer.Latest, wd.consumeDeviceType, wd.errorhandlerDeviceType, wd.config.Debug)
	if err != nil {
		return err
	}
	go wd.refreshTopics(refreshInterval)
	return nil
}

// refreshTopics picks up service topics that appear without a device type update,
// e.g. topics auto-created by a connector on their first event.
func (wd *Manager) refreshTopics(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-wd.parentContext.Done():
			return
		case <-ticker.C:
			err := wd.newConsumer()
			if err != nil {
				log.Logger.Error("refreshing service topics failed", attributes.ErrorKey, err)
			}
		}
	}
}

// newConsumer replaces the data consumer if the set of service topics changed.
// Partitions without a committed offset start at the oldest message, because a new
// topic already holds the events written before it was subscribed.
func (wd *Manager) newConsumer() error {
	wd.consumerMux.Lock()
	defer wd.consumerMux.Unlock()
	allTopics, err := wd.kafkaAdm.ListTopics()
	if err != nil {
		return err
	}
	topics := []string{}
	for _, topic := range allTopics {
		if strings.HasPrefix(topic, wd.config.ServiceTopicPrefix) {
			topics = append(topics, topic)
		}
	}
	if wd.consumer != nil && meta.EqualStringSlice(wd.consumer.GetTopics(), topics) {
		log.Logger.Debug("No changes in service topics", "count", len(topics))
		return nil
	}
	if len(topics) == 0 {
		log.Logger.Warn("No matching topics")
		return nil
	}
	log.Logger.Info("Got service topics", "count", len(topics))
	if wd.consumerCancel != nil {
		wd.consumerCancel()
	}
	ctx, cancel := context.WithCancel(wd.parentContext)
	wd.consumerCancel = cancel
	c, err := consumer.NewConsumer(ctx, wd.wg, wd.config.KafkaBootstrap, topics, wd.config.GroupId, consumer.Earliest,
		wd.consumeData, wd.errorhandlerData, wd.config.Debug)
	if err != nil {
		// the previous consumer is already cancelled; forgetting the topics makes the next refresh retry
		wd.consumer = nil
		return err
	}
	wd.consumer = c
	return nil
}

func (wd *Manager) consumeDeviceType(_ string, _ []*sarama.ConsumerMessage) error {
	log.Logger.Info("Received device type update, updating consumer if needed")
	log.Logger.Info("Waiting for topic adjustments")
	time.Sleep(5 * time.Second) // wait for topic adjustments
	err := wd.newConsumer()
	if err != nil {
		return err
	}
	log.Logger.Info("Update done")
	return nil
}

func (wd *Manager) errorhandlerDeviceType(err error, _ *consumer.Consumer, _ string) {
	log.Logger.Error("consuming device type update failed", attributes.ErrorKey, err)
}

func (wd *Manager) consumeData(_ string, msgs []*sarama.ConsumerMessage) error {
	envelopes := make([]meta.Envelope, len(msgs))
	timestamps := make([]time.Time, len(msgs))
	for i, msg := range msgs {
		var envelope meta.Envelope
		err := json.Unmarshal(msg.Value, &envelope)
		if err != nil {
			return err
		}
		envelopes[i] = envelope
		timestamps[i] = msg.Timestamp
	}
	if len(envelopes) == 0 {
		return nil
	}
	service, code, err := wd.memcached.GetService(envelopes[0].ServiceId)
	if err != nil {
		if code == http.StatusNotFound {
			log.Logger.Warn("Service not found, ignoring messages", "service_id", envelopes[0].ServiceId)
			return nil
		}
		return err
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()
		sErr := wd.psqlPublisher.Publish(envelopes, timestamps, service)
		if sErr != nil {
			err = sErr
		}
	}()

	var sizes []int

	go func() {
		defer wg.Done()
		var sErr error
		sizes, sErr = wd.memcached.Publish(envelopes, timestamps, service)
		if sErr != nil {
			err = sErr
		}
	}()

	wg.Wait()
	if err != nil {
		return err
	}

	go collectMetrics(envelopes, sizes)
	return nil
}

func (wd *Manager) errorhandlerData(err error, _ *consumer.Consumer, topic string) {
	log.Logger.Error("consuming data from topic failed", "topic", topic, attributes.ErrorKey, err)
	panic(err)
}

func collectMetrics(envelopes []meta.Envelope, sizes []int) {
	writtenCounter.Add(float64(len(envelopes)))

	for i, envelope := range envelopes {
		writtenCounterDevices.WithLabelValues(envelope.DeviceId).Add(1)
		writtenBytesCounterDevices.WithLabelValues(envelope.DeviceId).Add(float64(sizes[i]))
	}
}
