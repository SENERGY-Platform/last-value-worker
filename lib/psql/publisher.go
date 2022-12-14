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

package psql

import (
	"context"
	"fmt"
	"github.com/SENERGY-Platform/converter/lib/converter"
	"github.com/SENERGY-Platform/last-value-worker/lib/memcached"
	"github.com/SENERGY-Platform/last-value-worker/lib/meta"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"log"
	"strings"
	"sync"
	"time"
)

type Publisher struct {
	db                               *pgxpool.Pool
	debug                            bool
	serviceIdTimeCharacteristicCache map[string]characteristicIdTimestamp
	conv                             *converter.Converter
	memcached                        *memcached.Memcached
}

type characteristicIdTimestamp struct {
	CharacteristicId string
	Timestamp        time.Time
}

var ConnectionTimeout = 10 * time.Second

func New(postgresHost string, postgresPort int, postgresUser string, postgresPw string, postgresDb string, debugLog bool, memcached *memcached.Memcached, wg *sync.WaitGroup, basectx context.Context) (*Publisher, error) {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", postgresHost,
		postgresPort, postgresUser, postgresPw, postgresDb)

	config, err := pgxpool.ParseConfig(psqlconn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 50

	conv, err := converter.New()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(basectx)
	timeout, timeoutcancel := context.WithTimeout(basectx, ConnectionTimeout)
	defer timeoutcancel()
	go func() {
		<-timeout.Done()
		if timeout.Err() != context.Canceled {
			log.Println("ERROR: psql publisher connection timeout")
			cancel()
		}
	}()

	db, err := pgxpool.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}

	err = db.Ping(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	wg.Add(1)
	go func() {
		<-ctx.Done()
		db.Close()
		wg.Done()
	}()
	return &Publisher{
		db:                               db,
		debug:                            debugLog,
		serviceIdTimeCharacteristicCache: map[string]characteristicIdTimestamp{},
		conv:                             conv,
		memcached:                        memcached,
	}, nil
}

var slowProducerTimeout = 2 * time.Second

func (publisher *Publisher) Publish(mixedEnvelopes []meta.Envelope, mixedTimestamps []time.Time, service meta.Service) (err error) {
	if len(mixedEnvelopes) != len(mixedTimestamps) {
		log.Fatalln("FATAL: Expect same length envelopes and timestamps")
	}
	start := time.Now()

	deviceEnvelopes := make(map[string][]meta.Envelope)
	deviceTimestamps := make(map[string][]time.Time)

	for i, envelope := range mixedEnvelopes {
		envelopes, ok := deviceEnvelopes[envelope.DeviceId]
		if !ok {
			envelopes = []meta.Envelope{}
		}
		envelopes = append(envelopes, envelope)
		deviceEnvelopes[envelope.DeviceId] = envelopes

		timestamps, ok := deviceTimestamps[envelope.DeviceId]
		if !ok {
			timestamps = []time.Time{}
		}
		timestamps = append(timestamps, mixedTimestamps[i])
		deviceTimestamps[envelope.DeviceId] = timestamps
	}

	shortServiceId, err := ShortenId(service.Id)
	if err != nil {
		return err
	}

	fieldNames := []string{"time"}

	for _, output := range service.Outputs {
		fieldNames = append(fieldNames, meta.ParseContentVariable(output.ContentVariable, "")...)
	}

	for deviceId, envelopes := range deviceEnvelopes {
		shortDeviceId, err := ShortenId(deviceId)
		if err != nil {
			return err
		}
		table := "device:" + shortDeviceId + "_" + "service:" + shortServiceId
		rows := make([]string, len(envelopes))
		for i, envelope := range envelopes {
			values := make([]string, len(fieldNames))
			m := flatten(envelope.Value)

			t := deviceTimestamps[deviceId][i]

			tOverride, err := publisher.memcached.GetTimestampFromMessage(envelopes[i].Value, service)
			if err != nil {
				return err
			}
			if tOverride != nil {
				t = *tOverride
			}

			for j, fieldName := range fieldNames {
				if j == 0 {
					values[j] = "'" + t.UTC().Format(time.RFC3339Nano) + "'"
				} else {
					v, ok := m[fieldName]
					if !ok || v == nil {
						values[j] = "NULL"
					} else {
						values[j] = fmt.Sprintf("%v", v)
					}
				}
			}
			rows[i] = "(" + strings.Join(values, ", ") + ")"
		}
		query := "INSERT INTO \"" + table + "\" (\""
		query += strings.Join(fieldNames, "\", \"") + "\") VALUES " + strings.Join(rows, ", ") + ";"
		_, err = publisher.db.Exec(context.Background(), query)
		if err != nil {
			return err
		}
		if publisher.debug {
			log.Println("Postgres publishing took ", time.Since(start))
		}
		if slowProducerTimeout > 0 && time.Since(start) >= slowProducerTimeout {
			log.Println("WARNING: finished slow timescale publisher call", time.Since(start), deviceId, service.Id)
		}
	}
	return err
}

func flatten(m map[string]interface{}) (values map[string]interface{}) {
	values = make(map[string]interface{})
	for k, v := range m {
		switch child := v.(type) {
		case map[string]interface{}:
			nm := flatten(child)
			for nk, nv := range nm {
				values[k+"."+nk] = nv
			}
		case string:
			values[k] = "'" + v.(string) + "'"
		default:
			values[k] = v
		}
	}
	return values
}
