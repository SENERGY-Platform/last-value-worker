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

package memcached

import (
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/converter/lib/converter/characteristics"
	"github.com/SENERGY-Platform/last-value-worker/lib/meta"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"strings"
	"time"
)

func (this *Memcached) GetTimestampFromMessage(message map[string]interface{}, service meta.Service) (t *time.Time, err error) {
	for _, attr := range service.Attributes {
		if attr.Key == meta.TimeAttributeKey && len(attr.Value) > 0 {
			cachedItem, err := this.mc.Get("time_characteristic_id" + service.Id)
			timeCharacteristicId := ""
			if err != nil {
				pathParts := strings.Split(attr.Value, ".")
				for _, output := range service.Outputs {
					if output.ContentVariable.Name != pathParts[0] {
						continue
					}
					timeContentVariable := meta.GetDeepContentVariable(output.ContentVariable, pathParts[1:])
					if timeContentVariable == nil {
						return nil, errors.New("Can't find content variable with path " + attr.Value)
					}
					timeCharacteristicId = timeContentVariable.CharacteristicId
					err = this.mc.Set(&memcache.Item{
						Key:        "time_characteristic_id" + service.Id,
						Value:      []byte(timeCharacteristicId),
						Expiration: 5 * 60,
					})
					if err != nil {
						log.Println("WARNING: Could not wrote to memcached: " + err.Error())
					}
				}
			} else {
				timeCharacteristicId = string(cachedItem.Value)
			}
			timeVal := meta.GetDeepValue(message, strings.Split(attr.Value, "."))
			if timeVal == nil {
				return nil, errors.New("Can't find value with path " + attr.Value + " in message")
			}
			timeVal, err = this.conv.Cast(timeVal, timeCharacteristicId, characteristics.UnixNanoSeconds)
			if err != nil {
				return nil, err
			}
			t := time.Unix(0, timeVal.(int64))
			return &t, nil
		}
	}
	return nil, nil
}

func (this *Memcached) GetService(serviceId string) (service meta.Service, err error) {
	cachedItem, err := this.mc.Get("service_" + serviceId)
	if err == nil {
		err = json.Unmarshal(cachedItem.Value, &service)
		if err != nil {
			return service, err
		}
	} else {
		service, err = meta.GetService(serviceId, this.config.DeviceRepoUrl)
		if err != nil {
			return service, err
		}
		bytes, err := json.Marshal(service)
		if err != nil {
			return service, err
		}
		err = this.mc.Set(&memcache.Item{
			Key:        "service_" + service.Id,
			Value:      bytes,
			Expiration: 5 * 60,
		})
		if err != nil {
			log.Println("WARNING: " + err.Error())
			err = nil
		}
	}
	return service, err
}
