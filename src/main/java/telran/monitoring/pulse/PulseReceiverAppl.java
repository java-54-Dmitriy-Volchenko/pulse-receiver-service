package telran.monitoring.pulse;

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import telran.monitoring.pulse.dto.SensorData;

public class PulseReceiverAppl {
    private static final int PORT = 5004;
    private static final int MAX_BUFFER_SIZE = 1500;

    private static final Logger logger = LoggerFactory.getLogger(PulseReceiverAppl.class);

    private static final String LOGGING_LEVEL = System.getenv().getOrDefault("LOGGING_LEVEL", "INFO");
    private static final int MAX_THRESHOLD_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("MAX_THRESHOLD_PULSE_VALUE", "210"));
    private static final int MIN_THRESHOLD_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("MIN_THRESHOLD_PULSE_VALUE", "40"));
    private static final int WARN_MAX_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("WARN_MAX_PULSE_VALUE", "180"));
    private static final int WARN_MIN_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("WARN_MIN_PULSE_VALUE", "55"));

    static DatagramSocket socket;
    private static final DynamoDbClient dynamoDbClient = DynamoDbClient.builder()
            .region(Region.US_EAST_1) 
            .build();

    private static final String TABLE_NAME = "pulse_values";

    public static void main(String[] args) throws Exception {
        logger.info("[INFO] Using DynamoDB table: {}", TABLE_NAME);
        logger.info("[INFO] Actual logging level: {}", LOGGING_LEVEL);
        
        logger.info("[CONFIG] LOGGING_LEVEL: {}", LOGGING_LEVEL);
        logger.info("[CONFIG] MAX_THRESHOLD_PULSE_VALUE: {}", MAX_THRESHOLD_PULSE_VALUE);
        logger.info("[CONFIG] MIN_THRESHOLD_PULSE_VALUE: {}", MIN_THRESHOLD_PULSE_VALUE);
        logger.info("[CONFIG] WARN_MAX_PULSE_VALUE: {}", WARN_MAX_PULSE_VALUE);
        logger.info("[CONFIG] WARN_MIN_PULSE_VALUE: {}", WARN_MIN_PULSE_VALUE);

        socket = new DatagramSocket(PORT);
        byte[] buffer = new byte[MAX_BUFFER_SIZE];

        while (true) {
            DatagramPacket packet = new DatagramPacket(buffer, MAX_BUFFER_SIZE);
            socket.receive(packet);
            processReceivedData(buffer, packet);
        }
    }

    private static void processReceivedData(byte[] buffer, DatagramPacket packet) {
        String json = new String(Arrays.copyOf(buffer, packet.getLength()), StandardCharsets.UTF_8);

        SensorData sensorData = SensorData.getSensorData(json);

        logger.debug("[DEBUG] Received SensorData: {}", sensorData);

        if (sensorData.value() > MAX_THRESHOLD_PULSE_VALUE) {
            logger.error("[SEVERE] Critical pulse: {}", sensorData);
        } else if (sensorData.value() < MIN_THRESHOLD_PULSE_VALUE) {
            logger.error("[SEVERE] Dangerously low pulse: {}", sensorData);
        } else if (sensorData.value() > WARN_MAX_PULSE_VALUE) {
            logger.warn("[WARNING] High pulse: {}", sensorData);
        } else if (sensorData.value() < WARN_MIN_PULSE_VALUE) {
            logger.warn("[WARNING] Low pulse: {}", sensorData);
        }

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("seqNumber", AttributeValue.builder().n(String.valueOf(sensorData.seqNumber())).build());
        item.put("patientId", AttributeValue.builder().n(String.valueOf(sensorData.patientId())).build());
        item.put("value", AttributeValue.builder().n(String.valueOf(sensorData.value())).build());
        item.put("timestamp", AttributeValue.builder().n(String.valueOf(sensorData.timestamp())).build());

        PutItemRequest request = PutItemRequest.builder()
                .tableName(TABLE_NAME)
                .item(item)
                .build();

        try {
            dynamoDbClient.putItem(request);
            logger.debug("[DEBUG] Successfully put data for patientId: {}, timestamp: {}",
                    sensorData.patientId(), sensorData.timestamp());
        } catch (Exception e) {
            logger.error("[SEVERE] Failed to put item to DynamoDB: " + e.getMessage(), e);
        }
    }
}