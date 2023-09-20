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

package docker

import (
	"context"
	_ "github.com/lib/pq"
	"github.com/testcontainers/testcontainers-go"
	"log"
	"strconv"
	"sync"
)

func Tableworker(ctx context.Context, wg *sync.WaitGroup, postgresHost string, postgresPort int, postgresUser string, postgresPw string, postgresDb string, kafkaBootstrap string, deviceManagerUrl string) (err error) {
	log.Println("start tableworker")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:           "ghcr.io/senergy-platform/timescale-tableworker:dev",
			AlwaysPullImage: true,
			Env: map[string]string{
				"POSTGRES_PW":                 postgresPw,
				"POSTGRES_HOST":               postgresHost,
				"POSTGRES_DB":                 postgresDb,
				"POSTGRES_USER":               postgresUser,
				"POSTGRES_PORT":               strconv.Itoa(postgresPort),
				"KAFKA_BOOTSTRAP":             kafkaBootstrap,
				"DEVICE_MANAGER_URL":          deviceManagerUrl,
				"USE_DISTRIBUTED_HYPERTABLES": "false",
				"DEBUG":                       "true",
			},
		},
		Started: true,
	})
	if err != nil {
		return err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container tableworker", c.Terminate(context.Background()))
	}()

	return err
}
