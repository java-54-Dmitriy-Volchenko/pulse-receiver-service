package telran.monitoring.pulse;
import java.net.*;
import java.util.Arrays;
import java.util.logging.*;


import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.*;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec;

import telran.monitoring.pulse.dto.SensorData;


public class PulseReceiverAppl {
private static final int PORT = 5004;
private static final int MAX_BUFFER_SIZE = 1500;

private static final Logger logger = Logger.getLogger(PulseReceiverAppl.class.getName());

private static final String LOGGING_LEVEL = System.getenv().getOrDefault("LOGGING_LEVEL", "INFO");
private static final int MAX_THRESHOLD_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("MAX_THRESHOLD_PULSE_VALUE", "210"));
private static final int MIN_THRESHOLD_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("MIN_THRESHOLD_PULSE_VALUE", "40"));
private static final int WARN_MAX_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("WARN_MAX_PULSE_VALUE", "180"));
private static final int WARN_MIN_PULSE_VALUE = Integer.parseInt(System.getenv().getOrDefault("WARN_MIN_PULSE_VALUE", "55"));

static DatagramSocket socket;
static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
								.withRegion(Regions.US_EAST_1) 
								.build();
static DynamoDB dynamo = new DynamoDB(client);
static Table table = dynamo.getTable("pulse_values");

	
	public static void main(String[] args) throws Exception{
		
		 Level logLevel = Level.parse(LOGGING_LEVEL);
	        logger.setLevel(logLevel);
	        
	

	        
	        logger.log(Level.INFO, "Using DynamoDB table: {0}", table.getTableName());
	        logger.log(Level.INFO, "Actual logging level: {0}", LOGGING_LEVEL);
	      
	        logger.log(Level.CONFIG, "LOGGING_LEVEL: {0}", LOGGING_LEVEL);
	        logger.log(Level.CONFIG, "MAX_THRESHOLD_PULSE_VALUE: {0}", MAX_THRESHOLD_PULSE_VALUE);
	        logger.log(Level.CONFIG, "MIN_THRESHOLD_PULSE_VALUE: {0}", MIN_THRESHOLD_PULSE_VALUE);
	        logger.log(Level.CONFIG, "WARN_MAX_PULSE_VALUE: {0}", WARN_MAX_PULSE_VALUE);
	        logger.log(Level.CONFIG, "WARN_MIN_PULSE_VALUE: {0}", WARN_MIN_PULSE_VALUE);
		
		socket  = new DatagramSocket(PORT);
		byte [] buffer = new byte[MAX_BUFFER_SIZE];
		
		while(true) {
			DatagramPacket packet = new DatagramPacket(buffer, MAX_BUFFER_SIZE);
			socket.receive(packet);
			processReceivedData(buffer, packet);
		}
		

	}
	
	private static void processReceivedData(byte[] buffer,
			DatagramPacket packet) {
		 String json = new String(Arrays.copyOf(buffer, packet.getLength()));
	        
	        
	        SensorData sensorData = SensorData.getSensorData(json);

	       
	        logger.log(Level.FINE, "Received SensorData: {0}", sensorData);

	        
	        if (sensorData.value() > MAX_THRESHOLD_PULSE_VALUE) {
	            logger.log(Level.SEVERE, "Critical pulse: {0}", sensorData);
	        } else if (sensorData.value() < MIN_THRESHOLD_PULSE_VALUE) {
	            logger.log(Level.SEVERE, "Dangerously low pulse: {0}", sensorData);
	        } else if (sensorData.value() > WARN_MAX_PULSE_VALUE) {
	            logger.log(Level.WARNING, "High pulse: {0}", sensorData);
	        } else if (sensorData.value() < WARN_MIN_PULSE_VALUE) {
	            logger.log(Level.WARNING, "Low pulse: {0}", sensorData);
	        }

	       table.putItem(new PutItemSpec().withItem(Item.fromJSON(sensorData.toString())));
	      
	       logger.log(Level.FINER, "Putting data for patientId: {0}, timestamp: {1}",
	                new Object[]{sensorData.patientId(), sensorData.timestamp()});
		
		
	}

}