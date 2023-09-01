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

package kafkaAdmin

import (
	"github.com/IBM/sarama"
	"github.com/SENERGY-Platform/last-value-worker/lib/config"
)

type KafkaAdmin struct {
	config *config.Config
}

func New(config *config.Config) (*KafkaAdmin, error) {
	return &KafkaAdmin{
		config: config,
	}, nil
}

func (ka *KafkaAdmin) ListTopics() ([]string, error) {
	adm, err := ka.getAdmin()
	if err != nil {
		return nil, err
	}
	topics, err := adm.ListTopics()
	if err != nil {
		return nil, err
	}
	res := []string{}
	for topic := range topics {
		res = append(res, topic)
	}
	return res, nil
}

func (ka *KafkaAdmin) getAdmin() (admin sarama.ClusterAdmin, err error) {
	sconfig := sarama.NewConfig()
	sconfig.Version = sarama.V2_4_0_0
	return sarama.NewClusterAdmin([]string{ka.config.KafkaBootstrap}, sconfig)
}
