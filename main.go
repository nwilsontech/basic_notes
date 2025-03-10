package main

import (
    "context"
    "crypto/tls"
    "crypto/x509"
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "sync"
    "time"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/elastic/go-elasticsearch/v8"
    "github.com/elastic/go-elasticsearch/v8/esutil"
)

type Config struct {
    KafkaConfig        KafkaConfig
    ElasticsearchConfig ElasticsearchConfig
    BatchSize          int
    BatchTimeout       time.Duration
}

type KafkaConfig struct {
    BootstrapServers string
    Topics           []string
    GroupID          string
    AutoOffsetReset  string
}

type ElasticsearchConfig struct {
    Addresses     []string
    Username      string
    Password      string
    CACert        string
    SkipTLSVerify bool
    EnableRetry   bool
    MaxRetries    int
    RetryTimeout  time.Duration
}

type Message struct {
    Topic     string    `json:"topic"`
    Payload   string    `json:"payload"`
    Timestamp time.Time `json:"timestamp"`
}

type Consumer struct {
    messages chan *Message
    config   *Config
    es       *elasticsearch.Client
    bi       esutil.BulkIndexer
    consumer *kafka.Consumer
}

func newElasticsearchClient(config ElasticsearchConfig) (*elasticsearch.Client, error) {
    transport := http.Transport{
        TLSClientConfig: &tls.Config{
            InsecureSkipVerify: config.SkipTLSVerify,
        },
    }

    if config.CACert != "" {
        cert, err := os.ReadFile(config.CACert)
        if err != nil {
            return nil, fmt.Errorf("error reading CA certificate: %v", err)
        }
        certPool := x509.NewCertPool()
        if ok := certPool.AppendCertsFromPEM(cert); !ok {
            return nil, fmt.Errorf("error parsing CA certificate")
        }
        transport.TLSClientConfig.RootCAs = certPool
    }

    retryBackoff := func(attempt int) time.Duration {
        return time.Duration(attempt) * 100 * time.Millisecond
    }

    cfg := elasticsearch.Config{
        Addresses:     config.Addresses,
        Username:      config.Username,
        Password:      config.Password,
        Transport:     &transport,
        RetryOnStatus: []int{502, 503, 504, 429},
        MaxRetries:    config.MaxRetries,
        RetryBackoff:  retryBackoff,
        EnableRetryOnTimeout: config.EnableRetry,
        RetryTimeout:        config.RetryTimeout,
    }

    return elasticsearch.NewClient(cfg)
}

func newConsumer(config *Config) (*Consumer, error) {
    // Initialize Elasticsearch client
    es, err := newElasticsearchClient(config.ElasticsearchConfig)
    if err != nil {
        return nil, fmt.Errorf("error creating elasticsearch client: %v", err)
    }

    // Test Elasticsearch connection
    res, err := es.Info()
    if err != nil {
        return nil, fmt.Errorf("error connecting to elasticsearch: %v", err)
    }
    defer res.Body.Close()

    if res.IsError() {
        return nil, fmt.Errorf("error getting elasticsearch info: %s", res.String())
    }

    // Initialize Kafka consumer with plain configuration
    kc, err := kafka.NewConsumer(&kafka.ConfigMap{
        "bootstrap.servers":  config.KafkaConfig.BootstrapServers,
        "group.id":          config.KafkaConfig.GroupID,
        "auto.offset.reset": config.KafkaConfig.AutoOffsetReset,
        "security.protocol": "plaintext",
    })
    if err != nil {
        return nil, fmt.Errorf("error creating kafka consumer: %v", err)
    }

    // Initialize bulk indexer
    bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
        Client:        es,
        FlushBytes:    5e+6,
        FlushInterval: config.BatchTimeout,
        NumWorkers:    2,
        RetryOnStatus: []int{502, 503, 504, 429},
        RetryBackoff: func(attempt int) time.Duration {
            return time.Duration(attempt) * 100 * time.Millisecond
        },
        MaxRetries: config.ElasticsearchConfig.MaxRetries,
    })
    if err != nil {
        return nil, fmt.Errorf("error creating bulk indexer: %v", err)
    }

    return &Consumer{
        messages: make(chan *Message, config.BatchSize),
        config:   config,
        es:       es,
        bi:       bi,
        consumer: kc,
    }, nil
}

func (c *Consumer) consume(ctx context.Context) error {
    err := c.consumer.SubscribeTopics(c.config.KafkaConfig.Topics, nil)
    if err != nil {
        return fmt.Errorf("error subscribing to topics: %v", err)
    }

    for {
        select {
        case <-ctx.Done():
            return nil
        default:
            ev := c.consumer.Poll(100)
            if ev == nil {
                continue
            }

            switch e := ev.(type) {
            case *kafka.Message:
                c.messages <- &Message{
                    Topic:     *e.TopicPartition.Topic,
                    Payload:   string(e.Value),
                    Timestamp: e.Timestamp,
                }
            case kafka.Error:
                log.Printf("Error: %v: %v\n", e.Code(), e)
                if e.Code() == kafka.ErrAllBrokersDown {
                    return fmt.Errorf("all brokers are down")
                }
            }
        }
    }
}

func (c *Consumer) processBatch(ctx context.Context) {
    for msg := range c.messages {
        indexName := msg.Topic

        err := c.bi.Add(ctx, esutil.BulkIndexerItem{
            Action: "index",
            Index:  indexName,
            Body:   strings.NewReader(msg.Payload),
            OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
                log.Printf("Successfully indexed document to index %s", indexName)
            },
            OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
                if err != nil {
                    log.Printf("ERROR: %s", err)
                } else {
                    log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
                }
            },
        })

        if err != nil {
            log.Printf("Failed to add item to bulk indexer: %s", err)
        }
    }
}

func main() {
    config := &Config{
        KafkaConfig: KafkaConfig{
            BootstrapServers: "localhost:9092",
            Topics:           []string{"topic1", "topic2", "topic3"},
            GroupID:         "elasticsearch-consumer",
            AutoOffsetReset: "earliest",
        },
        ElasticsearchConfig: ElasticsearchConfig{
            Addresses:     []string{"https://elasticsearch:9200"},
            Username:      "elastic",
            Password:      "your-password",
            CACert:       "/path/to/ca.crt",
            SkipTLSVerify: false,
            EnableRetry:   true,
            MaxRetries:    3,
            RetryTimeout:  time.Second * 30,
        },
        BatchSize:    1000,
        BatchTimeout: time.Second * 5,
    }

    // Create consumer
    consumer, err := newConsumer(config)
    if err != nil {
        log.Fatalf("Error creating consumer: %v", err)
    }
    defer consumer.consumer.Close()
    defer consumer.bi.Close(context.Background())

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Handle graceful shutdown
    sigChan := make(chan os.Signal, 1)
    signal.Notify(sigChan, os.Interrupt)

    var wg sync.WaitGroup

    // Start processing messages
    wg.Add(1)
    go func() {
        defer wg.Done()
        consumer.processBatch(ctx)
    }()

    // Start consuming messages
    wg.Add(1)
    go func() {
        defer wg.Done()
        if err := consumer.consume(ctx); err != nil {
            log.Printf("Error consuming messages: %v", err)
            cancel()
        }
    }()

    // Wait for interrupt signal
    <-sigChan
    log.Println("Received interrupt signal, shutting down...")
    cancel()
    wg.Wait()
}
