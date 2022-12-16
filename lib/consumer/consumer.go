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

package consumer

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"strconv"
	"strings"
	"sync"
)

const Latest = sarama.OffsetNewest
const Earliest = sarama.OffsetOldest

func NewConsumer(ctx context.Context, wg *sync.WaitGroup, kafkaBootstrap string, topics []string, groupId string, offset int64, listener func(topic string, msgs []*sarama.ConsumerMessage) error, errorhandler func(err error, consumer *Consumer, topic string), debug bool) (consumer *Consumer, err error) {
	consumer = &Consumer{ctx: ctx, wg: wg, kafkaBootstrap: kafkaBootstrap, topics: topics, listener: listener, errorhandler: errorhandler, offset: offset, ready: make(chan bool), groupId: groupId, internalWg: &sync.WaitGroup{}, debug: debug}
	err = consumer.start()
	return
}

type Consumer struct {
	count          int
	kafkaBootstrap string
	topics         []string
	ctx            context.Context
	wg             *sync.WaitGroup
	internalWg     *sync.WaitGroup
	listener       func(topic string, msgs []*sarama.ConsumerMessage) error
	errorhandler   func(err error, consumer *Consumer, topic string)
	mux            sync.Mutex
	offset         int64
	groupId        string
	ready          chan bool
	debug          bool
}

func (this *Consumer) GetTopics() []string {
	return this.topics
}

func (this *Consumer) start() error {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = this.offset
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	config.ChannelBufferSize = 65536

	client, err := sarama.NewConsumerGroup(strings.Split(this.kafkaBootstrap, ","), this.groupId, config)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-this.ctx.Done():
				log.Println("close kafka reader")
				return
			default:
				if err := client.Consume(this.ctx, this.topics, this); err != nil {
					log.Panicf("Error from consumer: %v", err)
				}
				// check if context was cancelled, signaling that the consumer should stop
				if this.ctx.Err() != nil {
					return
				}
				this.ready = make(chan bool)
			}
		}
	}()

	<-this.ready // Await till the consumer has been set up
	log.Println("Kafka consumer " + this.groupId + " up and running...")

	return err
}

func (this *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(this.ready)
	this.wg.Add(1)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (this *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Cleaning up kafka session " + this.groupId)
	this.internalWg.Wait()
	log.Println("Cleaned up kafka session " + this.groupId)
	this.wg.Done()
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (this *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	msgsChannel := claim.Messages()
	done := this.ctx.Done()
	var message *sarama.ConsumerMessage
	var open bool
	topicMsgs := make(map[string][]*sarama.ConsumerMessage)

	queueMsg := func(message *sarama.ConsumerMessage) {
		arr, ok := topicMsgs[message.Topic]
		if !ok {
			arr = []*sarama.ConsumerMessage{}
		}
		arr = append(arr, message)
		topicMsgs[message.Topic] = arr
	}

	handleQueuedMsgs := func() {
		this.internalWg.Add(1)
		wg := &sync.WaitGroup{}
		wg.Add(len(topicMsgs))
		for topic, msgs := range topicMsgs {
			topic := topic
			msgs := msgs
			if this.debug {
				log.Println("Got " + strconv.Itoa(len(msgs)) + " from topic " + topic)
			}
			go func() {
				err := this.listener(topic, msgs)
				if err != nil {
					this.errorhandler(err, this, topic)
				}
				wg.Done()
			}()
		}
		wg.Wait()
		if message != nil {
			session.MarkMessage(message, "")
			message = nil
		}
		topicMsgs = make(map[string][]*sarama.ConsumerMessage)
		this.internalWg.Done()
	}

	defer handleQueuedMsgs()

	for {
		select {
		case <-done:
			return nil
		case message, open = <-msgsChannel:
			if !open {
				return nil
			}

			queueMsg(message)
			for i := len(msgsChannel); i > 0; i-- {
				message, open = <-msgsChannel
				if !open {
					return nil
				}
				queueMsg(message)
			}
			handleQueuedMsgs()
		}
	}
}
