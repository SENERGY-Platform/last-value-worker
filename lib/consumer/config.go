/*
 * Copyright 2025 InfAI (CC SES)
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

package consumer

import (
	"time"

	"github.com/IBM/sarama"
)

func SaramaConfig() *sarama.Config {
	config := sarama.NewConfig()
	config.Version = sarama.V3_6_0_0
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategySticky()
	config.ChannelBufferSize = 65536
	config.Net.ReadTimeout = 120 * time.Second
	config.Net.WriteTimeout = 120 * time.Second
	//config.Producer.MaxMessageBytes *= 100
	//config.Consumer.Fetch.Default *= 100
	return config
}
