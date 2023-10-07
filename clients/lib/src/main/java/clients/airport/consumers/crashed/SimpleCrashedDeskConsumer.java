package clients.airport.consumers.crashed;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.AirportProducer.TerminalInfoSerializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Consumer which will print out terminals that haven't a STATUS event in a while.
 */
public class SimpleCrashedDeskConsumer extends AbstractInteractiveShutdownConsumer {

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "crashed-desks-simple");

		// Kafka will auto-commit every 5s based on the last poll() call
		props.put("enable.auto.commit", "true");

		Map<Integer, Instant> lastHeartbeat = new TreeMap<>();
        ArrayList<Integer> currentlyCrashed = new ArrayList<>();
        
		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Collections.singleton(AirportProducer.TOPIC_STATUS));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
		        Instant currentTime = Instant.now();

				// Create a list to store the records

				for (ConsumerRecord<Integer, TerminalInfo> r : records) {
				    Integer key = r.key();
				    TerminalInfo value = r.value();
				    Instant lastHb = lastHeartbeat.get(key);

				    if(lastHb == null){
				    	lastHeartbeat.put(key, Instant.ofEpochMilli(r.timestamp()));
				    }
				    else if (lastHb.isAfter(Instant.ofEpochMilli(r.timestamp()))){
				    	{};
				    }else{
				    	lastHeartbeat.put(key, Instant.ofEpochMilli(r.timestamp()));
				    };
				    // Iterate through the entries and find keys with values more than 12 seconds ago
			        
				}
				for (Map.Entry<Integer, Instant> entry : lastHeartbeat.entrySet()) {
		            Integer key = entry.getKey();
		            Instant value = entry.getValue();
		            Long between = Duration.between(value, currentTime).toSeconds();
		            
		            if (between >= 12 && (!currentlyCrashed.contains(key))) {
		                System.out.printf("Key " + key + " had its last heartbeat more than 12 seconds ago. \n");
		                currentlyCrashed.add(key);
		            } else if(between >= 12) {
		            	{}
		            } else if(between < 12) {
		            	currentlyCrashed.remove(key);
		            }
		            
		        }

			}
		} 
		// TODO: exercise
	}

	public static void main(String[] args) {
		new SimpleCrashedDeskConsumer().runUntilEnterIsPressed(System.in);
	}

}
