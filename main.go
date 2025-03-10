package main

import (
    "context"
    "crypto/tls"
    "encoding/json"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strings"
    "sync"
    "time"

    "github.com/Shopify/sarama"
    "github.com/elastic/go-elasticsearch/v8"
    "github.com/elastic/go-elasticsearch/v8/esutil"
)

type Config struct {
    KafkaBrokers      []string
    KafkaTopics       []string
    ElasticsearchConfig ElasticsearchConfig
    BatchSize         int
    BatchTimeout      time.Duration
}

type ElasticsearchConfig struct {
    Addresses    []string
    Username     string
    Password     string
    CACert       string // Path to CA certificate
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
    ready    chan bool
    messages chan *Message
    config   *Config
    es       *elasticsearch.Client
    bi       esutil.BulkIndexer
}

func newElasticsearchClient(config ElasticsearchConfig) (*elasticsearch.Client, error) {
    // Create transport with TLS configuration
    transport := http.Transport{
        TLSClientConfig: &tls.Config{
            InsecureSkipVerify: config.SkipTLSVerify,
        },
    }

    // If CA certificate is provided, load it
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
        Addresses: config.Addresses,
        Username:  config.Username,
        Password:  config.Password,
        Transport: &transport,
        RetryOnStatus: []int{502, 503, 504, 429}, // Retry on service unavailable and too many requests
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

    // Test the connection
    res, err := es.Info()
    if err != nil {
        return nil, fmt.Errorf("error connecting to elasticsearch: %v", err)
    }
    defer res.Body.Close()

    if res.IsError() {
        return nil, fmt.Errorf("error getting elasticsearch info: %s", res.String())
    }

    // Initialize bulk indexer
    bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
        Client:        es,
        FlushBytes:    5e+6,  // 5MB
        FlushInterval: config.BatchTimeout,
        NumWorkers:    2,
        // Add retry options for bulk operations
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
        ready:    make(chan bool),
        messages: make(chan *Message, config.BatchSize),
        config:   config,
        es:       es,
        bi:       bi,
    }, nil
}

// ... (previous ConsumeClaim, Setup, and Cleanup methods remain the same)

func (c *Consumer) processBatch(ctx context.Context) {
    for msg := range c.messages {
        // Create the indexing operation
        indexName := msg.Topic // Use topic name as index name
        
        // Add retry mechanism for individual document indexing
        err := c.bi.Add(ctx, esutil.BulkIndexerItem{
            Action:     "index",
            Index:      indexName,
            Body:       strings.NewReader(msg.Payload),
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
        KafkaBrokers: []string{"localhost:9092"},
        KafkaTopics:  []string{"topic1", "topic2", "topic3"},
        ElasticsearchConfig: ElasticsearchConfig{
            Addresses:     []string{"https://elasticsearch:9200"},
            Username:      "elastic",
            Password:      "your-password",
            CACert:       "/path/to/ca.crt",  // Path to CA certificate if needed
            SkipTLSVerify: false,             // Set to true to skip certificate verification (not recommended for production)
            EnableRetry:   true,
            MaxRetries:    3,
            RetryTimeout:  time.Second * 30,
        },
        BatchSize:    1000,
        BatchTimeout: time.Second * 5,
    }

    // Initialize Sarama config
    saramaConfig := sarama.NewConfig()
    saramaConfig.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
    saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

    // Create consumer
    consumer, err := newConsumer(config)
    if err != nil {
        log.Fatalf("Error creating consumer: %v", err)
    }

    // ... (rest of the main function remains the same)
}
