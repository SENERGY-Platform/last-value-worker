/*
 * Copyright 2023 InfAI (CC SES)
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

package test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/IBM/sarama"
	"github.com/SENERGY-Platform/converter/lib/converter/characteristics"
	"github.com/SENERGY-Platform/last-value-worker/lib"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	"github.com/SENERGY-Platform/last-value-worker/lib/meta"
	"github.com/SENERGY-Platform/last-value-worker/lib/test/docker"
	"github.com/SENERGY-Platform/last-value-worker/lib/test/mock/iot"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/bradfitz/gomemcache/memcache"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIntegration(t *testing.T) {
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conf, err := config.Load("../../config.json")
	if err != nil {
		t.Error(err)
		return
	}
	conf.Debug = true

	_, zkIp, err := docker.Zookeeper(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	zkUrl := zkIp + ":2181"

	conf.KafkaBootstrap, err = docker.Kafka(ctx, wg, zkUrl)
	if err != nil {
		t.Error(err)
		return
	}

	conf.DeviceRepoUrl, err = iot.Mock(ctx, conf.KafkaBootstrap)
	if err != nil {
		t.Error(err)
		return
	}

	conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb, err = docker.Timescale(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}

	parsedDeviceRepoUrl, err := url.Parse(conf.DeviceRepoUrl)
	if err != nil {
		t.Error(err)
		return
	}

	hostIp, err := docker.GetHostIp(ctx)
	if err != nil {
		t.Error(err)
		return
	}

	dockerDeviceRepoUrl := "http://" + hostIp + ":" + parsedDeviceRepoUrl.Port()

	err = docker.Tableworker(ctx, wg, conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb, conf.KafkaBootstrap, dockerDeviceRepoUrl)
	if err != nil {
		t.Error(err)
		return
	}

	_, cacheIp, err := docker.Memcached(ctx, wg)
	if err != nil {
		t.Error(err)
		return
	}
	conf.MemcachedUrls = []string{cacheIp + ":11211"}

	_, err = lib.Start(conf, ctx)
	if err != nil {
		t.Error(err)
		return
	}

	dt := models.DeviceType{
		Name: "foo",
		Services: []models.Service{
			{
				Name:        "sepl_get",
				LocalId:     "sepl_get",
				Description: "sepl_get",
				Attributes: []models.Attribute{
					{
						Key:   "senergy/time_path",
						Value: "metrics.updateTime",
					},
				},
				Outputs: []models.Content{
					{
						Serialization: "json",
						ContentVariable: models.ContentVariable{
							Name: "metrics",
							Type: models.Structure,
							SubContentVariables: []models.ContentVariable{
								{
									Name:             "updateTime",
									Type:             models.Integer,
									CharacteristicId: characteristics.UnixSeconds,
								},
								{
									Name: "level",
									Type: models.Integer,
								},
								{
									Name:          "level_unit",
									Type:          models.String,
									UnitReference: "level",
								},
								{
									Name: "title",
									Type: models.String,
								},
								{
									Name: "missing",
									Type: models.String,
								},
							},
						},
					},
					{
						Serialization: "plain-text",
						ContentVariable: models.ContentVariable{
							Name: "other_var",
							Type: models.String,
						},
					},
				},
			},
		},
	}
	dt.GenerateId()

	d := models.Device{
		Name:         "multipart",
		LocalId:      "multipart",
		DeviceTypeId: dt.Id,
	}
	d.GenerateId()

	//send envelope before creating device/device-type to ensure the service topic exists and can be found by (wd *Manager) newConsumer()
	envelope := meta.Envelope{
		DeviceId:  d.Id,
		ServiceId: dt.Services[0].Id,
		Value:     map[string]interface{}{"metrics": map[string]interface{}{"level": 42, "level_unit": "test2", "title": "event", "updateTime": 13}, "other_var": "foo"},
	}
	msg, err := json.Marshal(envelope)
	if err != nil {
		t.Error(err)
		return
	}

	sconf := sarama.NewConfig()
	sconf.Producer.Retry.Max = 5
	sconf.Producer.RequiredAcks = sarama.WaitForAll
	sconf.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer([]string{conf.KafkaBootstrap}, sconf)
	if err != nil {
		t.Error(err)
		return
	}

	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: serviceIdToTopic(envelope.ServiceId),
		Value: sarama.ByteEncoder(msg),
	})
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	err = createDeviceType(conf, dt)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	err = createDevice(conf, d)
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	//repeat produce, in case the consumer starts with the latest offset
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic: serviceIdToTopic(envelope.ServiceId),
		Value: sarama.ByteEncoder(msg),
	})
	if err != nil {
		t.Error(err)
		return
	}

	time.Sleep(10 * time.Second)

	mc := memcache.New(conf.MemcachedUrls...)
	item, err := mc.Get("device_" + envelope.DeviceId[:57] + "_service_" + envelope.ServiceId)
	if err != nil {
		t.Error(err)
		return
	}

	if !strings.Contains(string(item.Value), `"value":{"metrics":{"level":42,"level_unit":"test2","title":"event","updateTime":13},"other_var":"foo"}`) {
		t.Error(string(item.Value))
	}

	//TODO: check postgres
}

func createDeviceType(conf config.Config, deviceType models.DeviceType) (err error) {
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(deviceType)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", conf.DeviceRepoUrl+"/device-types/"+url.QueryEscape(deviceType.Id), b)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return errors.New("unexpected status-code")
	}
	return nil
}

func createDevice(conf config.Config, device models.Device) (err error) {
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(device)
	if err != nil {
		return err
	}
	req, err := http.NewRequest("PUT", conf.DeviceRepoUrl+"/devices/"+url.QueryEscape(device.Id), b)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if resp.StatusCode >= 300 {
		return errors.New("unexpected status-code")
	}
	return nil
}

func serviceIdToTopic(id string) string {
	id = strings.ReplaceAll(id, "#", "_")
	id = strings.ReplaceAll(id, ":", "_")
	return id
}
