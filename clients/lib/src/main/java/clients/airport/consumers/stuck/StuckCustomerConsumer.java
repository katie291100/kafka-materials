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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Simple consumer which detects started checkins which get stuck in the middle, and raises them as events.
 * Not smart enough to deal with repartitions (could miss the case when we repartition right in the middle
 * of a checkin having started).
 */
public class StuckCustomerConsumer extends AbstractInteractiveShutdownConsumer {

	private static final String TOPIC_STUCK_CUSTOMERS = "selfservice-stuck-customers";

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "stuck-customers-simple");
		props.put("enable.auto.commit", "true");

		Set<Integer> startedCheckins = new HashSet<>();

		try (
			KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer());
			KafkaProducer<Integer, String> producer = new KafkaProducer<>(props, new IntegerSerializer(), new StringSerializer())
		) {
			consumer.subscribe(Arrays.asList(AirportProducer.TOPIC_CHECKIN, AirportProducer.TOPIC_COMPLETED, AirportProducer.TOPIC_CANCELLED, AirportProducer.TOPIC_OUTOFORDER));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));

				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					switch (record.topic()) {
					case AirportProducer.TOPIC_CHECKIN:
						startedCheckins.add(record.key()); break;
					case AirportProducer.TOPIC_CANCELLED:
					case AirportProducer.TOPIC_COMPLETED:
						startedCheckins.remove(record.key()); break;
					case AirportProducer.TOPIC_OUTOFORDER:
						if (startedCheckins.remove(record.key())) {
							System.out.printf("%s: customer got stuck at terminal %d!%n", Instant.ofEpochMilli(record.timestamp()), record.key());
							producer.send(new ProducerRecord<>(TOPIC_STUCK_CUSTOMERS, record.key(), "Detected stuck customer at " + Instant.ofEpochMilli(record.timestamp())));
						}

						break;
					}
				}
			}
		}
	}

	public static void main(String[] args) {
		new StuckCustomerConsumer().runUntilEnterIsPressed(System.in);
	}

}
