package clients.airport.consumers.stuck;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

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
 * Detects started checkins which get stuck in the middle due to an OUT_OF_ORDER
 * event, and raises them as events.
 */
public class StuckCustomerConsumer extends AbstractInteractiveShutdownConsumer {

	private static final String TOPIC_STUCK_CUSTOMERS = "selfservice-stuck";

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "stuck-customers-simple");
		props.put("enable.auto.commit", "true");
		KafkaProducer<Integer, TerminalInfo>  producer = new KafkaProducer<>(props, new IntegerSerializer(), new TerminalInfoSerializer());

		Set<Integer> startedCheckins = new HashSet<>();


		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Arrays.asList(AirportProducer.TOPIC_CHECKIN, AirportProducer.TOPIC_OUTOFORDER, AirportProducer.TOPIC_CANCELLED, AirportProducer.TOPIC_COMPLETED));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(10));
			      
				// Create a list to store the records
		        ArrayList<ConsumerRecord<Integer, TerminalInfo>> recordList = new ArrayList<>();

		        // Iterate through the iterator and add records to the list
		        records.iterator().forEachRemaining(recordList::add);

		        // Sort the list based on record timestamps using a lambda expression
		        recordList.sort((record1, record2) -> Long.compare(record1.timestamp(), record2.timestamp()));

				for (ConsumerRecord<Integer, TerminalInfo> r : recordList) {
				    Integer key = r.key();
				    TerminalInfo value = r.value();
				    String consumerTopic = r.topic();
				    	
				    switch (consumerTopic.toString()) {
				    	case AirportProducer.TOPIC_CHECKIN:
				    		startedCheckins.add(key);
				    		break;
				    	case AirportProducer.TOPIC_COMPLETED:
				    		startedCheckins.remove(key);
				    		break;
				    	case AirportProducer.TOPIC_CANCELLED:
				    		startedCheckins.remove(key);
				    		break;
				    	case AirportProducer.TOPIC_OUTOFORDER:
				    		if(startedCheckins.contains(key)) {
				    			System.out.printf(String.format("Customer got stuck at Kiosk %d \n", key));
					    		startedCheckins.remove(key);
					    		TerminalInfo tInfo = new TerminalInfo();
								tInfo.stuck = true;
								producer.send(new ProducerRecord<>(AirportProducer.TOPIC_STUCK, key, tInfo));
				    		}
				    		break;
				    };

				}

			}
		} 
	}

	public static void main(String[] args) {
		new StuckCustomerConsumer().runUntilEnterIsPressed(System.in);
	}

}
