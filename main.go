package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	broker        = "localhost:9092"
	topic         = "your-topic"
	groupID       = "batch-consumer-group"
	batchSize     = 10
	batchTimeout  = 5 * time.Second
	pollTimeoutMs = 100 // milliseconds
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers":  broker,
		"group.id":           groupID,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": true,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %s", err)
	}
	defer consumer.Close()

	err = consumer.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		log.Fatalf("Failed to subscribe: %s", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go batchConsumer(ctx, consumer, wg)

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan
	log.Println("Shutting down...")
	cancel()
	wg.Wait()
}

func batchConsumer(ctx context.Context, consumer *kafka.Consumer, wg *sync.WaitGroup) {
	defer wg.Done()

	var (
		batch       []*kafka.Message
		timer       *time.Timer
		timerActive bool
		timerChan   <-chan time.Time
	)

	resetTimer := func() {
		if timer != nil && timerActive {
			timer.Stop()
		}
		timer = time.NewTimer(batchTimeout)
		timerChan = timer.C
		timerActive = true
	}

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				processBatch(batch)
			}
			return

		default:
			ev := consumer.Poll(pollTimeoutMs)
			if ev == nil {
				// Check timeout
				select {
				case <-timerChan:
					if len(batch) > 0 {
						processBatch(batch)
						batch = nil
						timerActive = false
					}
				default:
				}
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				batch = append(batch, e)
				if len(batch) == 1 {
					resetTimer()
				}
				if len(batch) >= batchSize {
					processBatch(batch)
					batch = nil
					timerActive = false
				}
			case kafka.Error:
				log.Printf("Kafka error: %v\n", e)
			default:
				log.Printf("Ignored: %v\n", e)
			}
		}
	}
}

func processBatch(batch []*kafka.Message) {
	go func(batch []*kafka.Message) {
		log.Printf("Processing batch of %d messages...\n", len(batch))
		for _, msg := range batch {
			fmt.Printf("Partition: %d, Offset: %d, Key: %s, Value: %s\n",
				msg.TopicPartition.Partition,
				msg.TopicPartition.Offset,
				string(msg.Key),
				string(msg.Value))
		}
	}(batch)
}
