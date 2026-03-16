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

package main

import (
	"context"
	"flag"

	"github.com/SENERGY-Platform/go-service-base/struct-logger/attributes"
	"github.com/SENERGY-Platform/last-value-worker/lib"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	_log "github.com/SENERGY-Platform/last-value-worker/lib/log"

	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configLocation := flag.String("config", "config.json", "configuration file")
	flag.Parse()

	conf, err := config.Load(*configLocation)
	if err != nil {
		log.Fatal("ERROR: unable to load config", err)
	}
	_log.Init(conf)

	ctx, cancel := context.WithCancel(context.Background())
	wg, err := lib.Start(conf, ctx)
	if err != nil {
		cancel()
		if wg != nil {
			wg.Wait()
		}
		_log.Logger.Error("unable to start lib", attributes.ErrorKey, err)
		log.Fatal(err)
	}

	var shutdownTime time.Time
	go func() {
		shutdown := make(chan os.Signal, 1)
		signal.Notify(shutdown, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
		sig := <-shutdown
		_log.Logger.Info("received shutdown signal", "signal", sig)
		shutdownTime = time.Now()
		cancel()
	}()

	wg.Wait()
	_log.Logger.Info("Shutdown complete", "duration", time.Since(shutdownTime))
}
