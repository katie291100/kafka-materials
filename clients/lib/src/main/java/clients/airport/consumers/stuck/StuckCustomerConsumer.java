package clients.airport.consumers.stuck;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Detects started checkins which get stuck in the middle due to an OUT_OF_ORDER
 * event, and raises them as events.
 */
public class StuckCustomerConsumer extends AbstractInteractiveShutdownConsumer {

	private static final String TOPIC_STUCK_CUSTOMERS = "selfservice-stuck-customers";

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "stuck-customers-simple");
		props.put("enable.auto.commit", "true");

		Set<Integer> startedCheckins = new HashSet<>();


		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Arrays.asList(AirportProducer.TOPIC_CHECKIN, AirportProducer.TOPIC_OUTOFORDER, AirportProducer.TOPIC_CANCELLED, AirportProducer.TOPIC_COMPLETED));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(10));

				for (ConsumerRecord<Integer, TerminalInfo> r : records) {
				    Integer key = r.key();
				    TerminalInfo value = r.value();
				    String consumerTopic = r.topic();
				    	
				    switch (consumerTopic.toString()) {
				    	case AirportProducer.TOPIC_CHECKIN:
				    		startedCheckins.add(key);
				    	case AirportProducer.TOPIC_COMPLETED:
				    		startedCheckins.remove(key);
				    	case AirportProducer.TOPIC_CANCELLED:
				    		startedCheckins.remove(key);
				    	case AirportProducer.TOPIC_OUTOFORDER:
				    		if(startedCheckins.contains(key)) {
				    			System.out.printf(String.format("Customer got stuck at Kiosk %d \n", key));
					    		startedCheckins.remove(key);
				    		}
				    		System.out.printf("System crashed");

				    };

				}

			}
		} 
		// TODO: exercise
	}

	public static void main(String[] args) {
		new StuckCustomerConsumer().runUntilEnterIsPressed(System.in);
	}

}
