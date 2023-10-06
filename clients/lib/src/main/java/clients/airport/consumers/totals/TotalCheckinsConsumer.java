package clients.airport.consumers.totals;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

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
 * Consumer that reports total numbers of started, completed, and cancelled
 * checkins. The first version is very simplistic and won't handle rebalancing.
 * This overall computation wouldn't scale well anyway, as it doesn't apply any
 * windows or split the input in any particular way.
 */
public class TotalCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "total-checkins");
		props.put("enable.auto.commit", "true");
		
		int started = 0, completed = 0, cancelled = 0;
		Instant mostRecentTime = Instant.ofEpochMilli(0);
		// TODO: exercise
		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Arrays.asList(AirportProducer.TOPIC_CHECKIN, AirportProducer.TOPIC_CANCELLED, AirportProducer.TOPIC_COMPLETED));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(10));
			    Thread.sleep(10000);

				for (ConsumerRecord<Integer, TerminalInfo> r : records) {
				    Integer key = r.key();
				    TerminalInfo value = r.value();
				    String consumerTopic = r.topic();
				    // Process the key and value as needed
				    	
				    switch (consumerTopic.toString()) {
				    	case AirportProducer.TOPIC_CHECKIN:
				    		started = started + 1;
				    	case AirportProducer.TOPIC_COMPLETED:
				    		completed = completed + 1;
				    	case AirportProducer.TOPIC_CANCELLED:
				    		cancelled = cancelled + 1;
				    };
				    if(Instant.ofEpochMilli(r.timestamp()).isAfter(mostRecentTime)) {
				    	mostRecentTime = Instant.ofEpochMilli(r.timestamp());
				    }
				}
				System.out.printf(String.format("%s Started: %d Completed: %d Cancelled: %d \n", mostRecentTime.toString(), started, completed, cancelled));

			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static void main(String[] args) {
		new TotalCheckinsConsumer().runUntilEnterIsPressed(System.in);
	}

}
