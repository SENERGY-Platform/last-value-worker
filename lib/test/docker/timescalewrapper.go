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
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"log"
	"strconv"
	"strings"
	"sync"
)

func Timescalewrapper(ctx context.Context, wg *sync.WaitGroup, postgresHost string, postgresPort int, postgresUser string, postgresPw string, postgresDb string, deviceRepoUrl string, permSearchUrl string, servingUrl string, memcacheUrls []string) (wrapperUrl string, err error) {
	log.Println("start timescalewrapper")
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:           "ghcr.io/senergy-platform/timescale-wrapper:dev",
			AlwaysPullImage: true,
			Env: map[string]string{
				"POSTGRES_PW":           postgresPw,
				"POSTGRES_HOST":         postgresHost,
				"POSTGRES_DB":           postgresDb,
				"POSTGRES_USER":         postgresUser,
				"POSTGRES_PORT":         strconv.Itoa(postgresPort),
				"DEVICE_REPO_URL":       deviceRepoUrl,
				"PERMISSION_SEARCH_URL": permSearchUrl,
				"SERVING_URL":           servingUrl,
				"MEMCACHED_URLS":        strings.Join(memcacheUrls, ","),
				"DEBUG":                 "true",
			},
			WaitingFor:   wait.ForListeningPort("8080/tcp"),
			ExposedPorts: []string{"8080"},
		},
		Started: true,
	})
	if err != nil {
		return wrapperUrl, err
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("DEBUG: remove container tableworker", c.Terminate(context.Background()))
	}()

	ipAddress, err := c.ContainerIP(ctx)
	if err != nil {
		return "", err
	}

	return "http://" + ipAddress + ":8080", err
}
