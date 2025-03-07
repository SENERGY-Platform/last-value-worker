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
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/SENERGY-Platform/converter/lib/converter/characteristics"
	"github.com/SENERGY-Platform/last-value-worker/lib"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	"github.com/SENERGY-Platform/last-value-worker/lib/meta"
	"github.com/SENERGY-Platform/last-value-worker/lib/test/docker"
	"github.com/SENERGY-Platform/last-value-worker/lib/test/mock/iot"
	"github.com/SENERGY-Platform/models/go/models"
	"github.com/bradfitz/gomemcache/memcache"
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

	wg2 := sync.WaitGroup{}
	mux := sync.Mutex{}

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		_, zkIp, err := docker.Zookeeper(ctx, wg)
		if err != nil {
			t.Error(err)
			return
		}
		zkUrl := zkIp + ":2181"
		kafkaBootstrap, err := docker.Kafka(ctx, wg, zkUrl)
		defer mux.Unlock()
		mux.Lock()
		conf.KafkaBootstrap = kafkaBootstrap
		if err != nil {
			t.Error(err)
			return
		}
	}()

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		postgresHost, postgresPort, postgresUser, postgresPw, postgresDb, err := docker.Timescale(ctx, wg)
		if err != nil {
			t.Error(err)
			return
		}
		defer mux.Unlock()
		mux.Lock()
		conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb = postgresHost, postgresPort, postgresUser, postgresPw, postgresDb
	}()

	hostIp := "host.docker.internal"
	permMockServ := &httptest.Server{
		Config: &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			json.NewEncoder(writer).Encode(true)
		})},
	}
	permMockServ.Listener, err = net.Listen("tcp4", ":")
	if err != nil {
		t.Error(err)
		return
	}
	permMockServ.Start()
	defer permMockServ.Close()

	parsedPermUrl, err := url.Parse(permMockServ.URL)
	if err != nil {
		t.Error(err)
		return
	}
	permUrl := "http://" + hostIp + ":" + parsedPermUrl.Port()

	wg2.Wait()
	conf.DeviceRepoUrl, err = iot.Mock(ctx, conf.KafkaBootstrap)
	if err != nil {
		t.Error(err)
		return
	}

	parsedDeviceRepoUrl, err := url.Parse(conf.DeviceRepoUrl)
	if err != nil {
		t.Error(err)
		return
	}

	dockerDeviceRepoUrl := "http://" + hostIp + ":" + parsedDeviceRepoUrl.Port()

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		err = docker.Tableworker(ctx, wg, conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb, conf.KafkaBootstrap, dockerDeviceRepoUrl)
		if err != nil {
			t.Error(err)
			return
		}
	}()

	wg2.Add(1)
	go func() {
		defer wg2.Done()
		cachePort, cacheIp, err := docker.Memcached(ctx, wg)
		if err != nil {
			t.Error(err)
			return
		}
		defer mux.Unlock()
		mux.Lock()
		conf.MemcachedUrls = []string{cacheIp + ":" + cachePort}
	}()

	wg2.Wait()
	timescaleWrapperUrl, err := docker.Timescalewrapper(ctx, wg, conf.PostgresHost, conf.PostgresPort, conf.PostgresUser, conf.PostgresPw, conf.PostgresDb, dockerDeviceRepoUrl, permUrl, "", conf.MemcachedUrls)
	if err != nil {
		t.Error(err)
		return
	}

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
								{
									Name: "listvariable",
									Type: models.List,
									SubContentVariables: []models.ContentVariable{
										{
											Name: "*",
											Type: models.Structure,
											SubContentVariables: []models.ContentVariable{
												{
													Name:                 "value",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
												{
													Name:                 "value2",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
											},
										},
									},
								},
								{
									Name: "listfixed",
									Type: models.List,
									SubContentVariables: []models.ContentVariable{
										{
											Name: "0",
											Type: models.Structure,
											SubContentVariables: []models.ContentVariable{
												{
													Name:                 "value",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
												{
													Name:                 "value2",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
											},
										},
										{
											Name: "1",
											Type: models.Structure,
											SubContentVariables: []models.ContentVariable{
												{
													Name:                 "value",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
												{
													Name:                 "value2",
													Type:                 models.Integer,
													CharacteristicId:     "urn:infai:ses:characteristic:a49a48fc-3a2c-4149-ac7f-1a5482d4c6e1",
													FunctionId:           "urn:infai:ses:measuring-function:f2769eb9-b6ad-4f7e-bd28-e4ea043d2f8b",
													AspectId:             "urn:infai:ses:aspect:a14c5efb-b0b6-46c3-982e-9fded75b5ab6",
													SerializationOptions: []string{models.SerializationOptionXmlAttribute},
												},
											},
										},
									},
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
		Value:     map[string]interface{}{"metrics": map[string]interface{}{"level": 42, "level_unit": "test2", "title": "event", "updateTime": 13, "listvariable": []map[string]interface{}{{"value": 12, "value2": 34}, {"value": 56, "value2": 78}, {"value": 90, "value2": 12}}, "listfixed": []map[string]interface{}{{"value": 12, "value2": 34}, {"value": 56, "value2": 78}}}, "other_var": "foo"},
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

	if !strings.Contains(string(item.Value), `"value":{"metrics":{"level":42,"level_unit":"test2","listfixed":[{"value":12,"value2":34},{"value":56,"value2":78}],"listvariable":[{"value":12,"value2":34},{"value":56,"value2":78},{"value":90,"value2":12}],"title":"event","updateTime":13},"other_var":"foo"}}`) {
		t.Error(string(item.Value))
	}

	lastValueResp, err := lastValueQuery(timescaleWrapperUrl, []map[string]interface{}{
		{
			"columnName": "metrics.level",
			"deviceId":   d.Id,
			"serviceId":  dt.Services[0].Id,
		},
		{
			"columnName": "other_var",
			"deviceId":   d.Id,
			"serviceId":  dt.Services[0].Id,
		},
		{
			"columnName": "metrics.listvariable",
			"deviceId":   d.Id,
			"serviceId":  dt.Services[0].Id,
		},
		{
			"columnName": "metrics.listfixed.1.value",
			"deviceId":   d.Id,
			"serviceId":  dt.Services[0].Id,
		},
	})
	if err != nil {
		t.Error(err)
		return
	}

	normalizedResponse := normalizeLastValueResp(lastValueResp)
	normalizedRequest := normalizeLastValueResp([]map[string]interface{}{{"value": 42.0}, {"value": "foo"}, {"value": []interface{}{map[string]interface{}{"value": 12.0, "value2": 34.0}, map[string]interface{}{"value": 56.0, "value2": 78.0}, map[string]interface{}{"value": 90.0, "value2": 12.0}}}, {"value": 56.0}})
	if !reflect.DeepEqual(normalizedResponse, normalizedRequest) {
		t.Errorf("%#v", lastValueResp)
		return
	}

}

func normalizeLastValueResp(in []map[string]interface{}) (out []map[string]interface{}) {
	for _, v := range in {
		v["time"] = ""
		out = append(out, v)
	}
	return out
}

func lastValueQuery(timescaleWrapperUrl string, query []map[string]interface{}) (result []map[string]interface{}, err error) {
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(query)
	if err != nil {
		return result, err
	}
	req, err := http.NewRequest("POST", timescaleWrapperUrl+"/last-values", b)
	if err != nil {
		return result, err
	}

	token := "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
	req.Header.Set("Authorization", token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		debug.PrintStack()
		return result, err
	}
	if resp.StatusCode >= 300 {
		temp, _ := io.ReadAll(resp.Body)
		return result, errors.New(fmt.Sprintf("unexpected status-code: %v, %v", resp.StatusCode, string(temp)))
	}
	err = json.NewDecoder(resp.Body).Decode(&result)
	if err != nil {
		debug.PrintStack()
		return result, err
	}
	return result, err
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
