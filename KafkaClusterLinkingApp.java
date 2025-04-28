package com.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.clients.admin.NewClientLink;
import org.apache.kafka.clients.admin.CreateClientLinkResult;

import java.util.*;

public class KafkaClusterLinkingApp {
    
    private static final String SOURCE_BROKER = "source-kafka:9092";
    private static final String DEST_BROKER = "destination-kafka:9092";
    private static final String LINK_NAME = "my-cluster-link";
    private static final String TOPIC_NAME = "my-topic";
    private static final String SOURCE_CLUSTER_ID = "lkc-sourceclusterid"; // You should get this from the source broker
    
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, DEST_BROKER);
        
        // Step 1: Create the Cluster Link
        try (AdminClient adminClient = AdminClient.create(props)) {
            createClusterLink(adminClient);
        }
        
        // Step 2: Mirror the topic
        try (AdminClient adminClient = AdminClient.create(props)) {
            mirrorTopic(adminClient);
        }
    }

    // Step 1: Create Cluster Link on destination Kafka cluster
    public static void createClusterLink(AdminClient adminClient) {
        Map<String, String> linkConfig = new HashMap<>();
        linkConfig.put("bootstrap.servers", SOURCE_BROKER);
        linkConfig.put("connection.mode", "INBOUND");
        linkConfig.put("security.protocol", "PLAINTEXT");
        linkConfig.put("source.cluster.id", SOURCE_CLUSTER_ID);

        NewClientLink clientLink = new NewClientLink(LINK_NAME, linkConfig);

        CreateClientLinkResult result = adminClient.createClientLinks(Collections.singleton(clientLink));
        
        try {
            result.all().get();  // Block until the operation is complete
            System.out.println("Cluster link '" + LINK_NAME + "' created successfully.");
        } catch (Exception e) {
            System.err.println("Failed to create Cluster Link: " + e.getMessage());
        }
    }

    // Step 2: Mirror a topic from the source to destination Kafka cluster
    public static void mirrorTopic(AdminClient adminClient) {
        NewMirrorTopic mirrorTopic = new NewMirrorTopic(TOPIC_NAME)
            .linkName(LINK_NAME);

        try {
            CreateMirrorTopicResult mirrorResult = adminClient.createMirrorTopics(Collections.singleton(mirrorTopic));
            mirrorResult.all().get();  // Block until the operation is complete
            System.out.println("Topic '" + TOPIC_NAME + "' mirrored successfully.");
        } catch (Exception e) {
            System.err.println("Failed to mirror topic: " + e.getMessage());
        }
    }
}
