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
	"github.com/SENERGY-Platform/last-value-worker/lib/meta"
	"github.com/bradfitz/gomemcache/memcache"
	"log"
	"time"
)

func (this *Memcached) Publish(envelopes []meta.Envelope, timestamps []time.Time, service meta.Service) (err error) {
	if len(envelopes) != len(timestamps) {
		log.Fatalln("FATAL: Expect same length envelopes and timestamps")
	}
	start := time.Now()
	for i := range envelopes {
		t := timestamps[i]
		tOverride, sErr := this.GetTimestampFromMessage(envelopes[i].Value, service)
		if sErr != nil {
			err = sErr
			return
		}
		if tOverride != nil {
			t = *tOverride
		}
		v := map[string]interface{}{
			"time":  t.UTC().Format(time.RFC3339Nano),
			"value": envelopes[i].Value,
		}
		bytes, sErr := json.Marshal(v)
		if sErr != nil {
			err = sErr
			return
		}
		sErr = this.mc.Set(&memcache.Item{
			Key:        "device_" + envelopes[i].DeviceId[:57] + "_service_" + envelopes[i].ServiceId,
			Value:      bytes,
			Expiration: 0, // no expiration
		})
		if sErr != nil {
			err = sErr
		}
	}
	if this.config.Debug {
		log.Println("Memcached publishing took ", time.Since(start))
	}

	return err
}
