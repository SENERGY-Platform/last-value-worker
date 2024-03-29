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
	"github.com/SENERGY-Platform/converter/lib/converter"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
	"github.com/bradfitz/gomemcache/memcache"
	"time"
)

type Memcached struct {
	mc     *memcache.Client
	conv   *converter.Converter
	config *config.Config
}

func New(c *config.Config) (*Memcached, error) {
	conv, err := converter.New()
	if err != nil {
		return nil, err
	}
	mc := memcache.New(c.MemcachedUrls...)
	mc.MaxIdleConns = 100
	mc.Timeout = time.Second
	return &Memcached{mc: mc, conv: conv, config: c}, nil

}
