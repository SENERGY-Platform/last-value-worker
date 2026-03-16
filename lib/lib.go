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

package lib

import (
	"context"
	"net/http"
	"sync"

	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	"github.com/SENERGY-Platform/last-value-worker/lib/log"
	"github.com/SENERGY-Platform/last-value-worker/lib/manager"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func Start(conf config.Config, ctx context.Context) (wg *sync.WaitGroup, err error) {
	wg = &sync.WaitGroup{}

	_, err = manager.New(&conf, ctx, wg)

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Logger.Info("Starting prometheus metrics on :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Logger.Warn("Metrics server exited", attributes.ErrorKey, err)
		}
	}()

	return wg, err
}
