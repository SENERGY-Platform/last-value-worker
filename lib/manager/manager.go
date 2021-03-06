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
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	"github.com/SENERGY-Platform/last-value-worker/lib/consumer"
	kafkaAdmin "github.com/SENERGY-Platform/last-value-worker/lib/kafka-admin"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"strconv"
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
	mc                 *memcache.Client

	statsCounter uint64
	statsMutex   sync.Mutex
}

func New(c *config.Config, ctx context.Context, wg *sync.WaitGroup) (*Manager, error) {
	if c == nil || ctx == nil {
		return nil, errors.New("nil argument")
	}
	kafkaAdm, err := kafkaAdmin.New(c)
	if err != nil {
		return nil, err
	}
	wd := &Manager{config: c, parentContext: ctx, kafkaAdm: kafkaAdm, wg: wg, mc: memcache.New(c.MemcachedUrls...)}
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
	go wd.statsLogger()
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
	if wd.consumer != nil && equalStringSlice(wd.consumer.GetTopics(), topics) {
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

func (wd *Manager) consumeDeviceType(_ string, _ []byte, _ time.Time) error {
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

func (wd *Manager) errorhandlerDeviceType(err error, _ *consumer.Consumer) {
	log.Println("ERROR consuming device type update: " + err.Error())
}

func (wd *Manager) consumeData(_ string, msg []byte, t time.Time) error {
	m := map[string]interface{}{}
	err := json.Unmarshal(msg, &m)
	if err != nil {
		return err
	}
	value, ok := m["value"]
	if !ok {
		return errors.New("unfamiliar message format: missing value")
	}
	deviceId, ok := m["device_id"].(string)
	if !ok {
		return errors.New("unfamiliar message format: missing device_id")
	}
	serviceId, ok := m["service_id"].(string)
	if !ok {
		return errors.New("unfamiliar message format: missing service_id")
	}
	v := map[string]interface{}{
		"time":  t.Format(time.RFC3339Nano),
		"value": value,
	}
	bytes, err := json.Marshal(v)
	if err != nil {
		return err
	}
	wd.statsMutex.Lock()
	wd.statsCounter++
	wd.statsMutex.Unlock()
	return wd.mc.Set(&memcache.Item{
		Key:        "device_" + deviceId + "_service_" + serviceId,
		Value:      bytes,
		Expiration: 0, // no expiration
	})
}

func (wd *Manager) errorhandlerData(err error, _ *consumer.Consumer) {
	log.Println("ERROR consuming data: " + err.Error())
}

func (wd *Manager) statsLogger() {
	for {
		time.Sleep(time.Minute)
		wd.statsMutex.Lock()
		log.Println("STATS Wrote " + strconv.FormatUint(wd.statsCounter, 10) + " entries in the last minute")
		wd.statsCounter = 0
		wd.statsMutex.Unlock()
	}
}

func equalStringSlice(a []string, b []string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a) != len(b) {
		return false
	}
	for _, aElem := range a {
		if !elemInSlice(aElem, b) {
			return false
		}
	}
	return true
}

func elemInSlice(elem string, slice []string) bool {
	for _, sliceElem := range slice {
		if elem == sliceElem {
			return true
		}
	}
	return false
}
