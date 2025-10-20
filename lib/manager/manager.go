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
	"github.com/IBM/sarama"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	"github.com/SENERGY-Platform/last-value-worker/lib/consumer"
	kafkaAdmin "github.com/SENERGY-Platform/last-value-worker/lib/kafka-admin"
	"github.com/SENERGY-Platform/last-value-worker/lib/memcached"
	"github.com/SENERGY-Platform/last-value-worker/lib/meta"
	"github.com/SENERGY-Platform/last-value-worker/lib/psql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	config             *config.Config
	consumer           *consumer.Consumer
	deviceTypeConsumer *consumer.Consumer
	consumerCancel     context.CancelFunc
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
	err := wd.newConsumer()
	if err != nil {
		return err
	}
	wd.deviceTypeConsumer, err = consumer.NewConsumer(wd.parentContext, wd.wg, wd.config.KafkaBootstrap, []string{wd.config.DeviceTypeTopic},
		wd.config.DeviceTypeGroupId, consumer.Latest, wd.consumeDeviceType, wd.errorhandlerDeviceType, wd.config.Debug)
	return err
}

func (wd *Manager) newConsumer() error {
	allTopics, err := wd.kafkaAdm.ListTopics()
	topics := []string{}
	for _, topic := range allTopics {
		if strings.HasPrefix(topic, wd.config.ServiceTopicPrefix) {
			topics = append(topics, topic)
		}
	}
	log.Printf("Got %d service topics\n", len(topics))
	if wd.consumer != nil && meta.EqualStringSlice(wd.consumer.GetTopics(), topics) {
		log.Println("No changes...")
		return nil
	}
	if len(topics) == 0 {
		log.Println("No matching topics")
		return err
	}
	if wd.consumerCancel != nil {
		wd.consumerCancel()
	}
	ctx, cancel := context.WithCancel(wd.parentContext)
	wd.consumerCancel = cancel
	wd.consumer, err = consumer.NewConsumer(ctx, wd.wg, wd.config.KafkaBootstrap, topics, wd.config.GroupId, consumer.Latest,
		wd.consumeData, wd.errorhandlerData, wd.config.Debug)
	return err
}

func (wd *Manager) consumeDeviceType(_ string, _ []*sarama.ConsumerMessage) error {
	log.Println("Received device type update, updating consumer if needed")
	log.Println("Waiting for topic adjustments....")
	time.Sleep(5 * time.Second) // wait for topic adjustments
	err := wd.newConsumer()
	if err != nil {
		return err
	}
	log.Println("Updated done")
	return nil
}

func (wd *Manager) errorhandlerDeviceType(err error, _ *consumer.Consumer, _ string) {
	log.Println("ERROR consuming device type update: " + err.Error())
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
			log.Println("WARN: Service " + envelopes[0].ServiceId + " not found, ignoring messages!")
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
	log.Fatalln("ERROR consuming data from topic " + topic + " : " + err.Error())
}

func collectMetrics(envelopes []meta.Envelope, sizes []int) {
	writtenCounter.Add(float64(len(envelopes)))

	for i, envelope := range envelopes {
		writtenCounterDevices.WithLabelValues(envelope.DeviceId).Add(1)
		writtenBytesCounterDevices.WithLabelValues(envelope.DeviceId).Add(float64(sizes[i]))
	}
}
