package clients.airport.consumers.crashed;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

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
 * Naive version of a 'possibly down' consumer, which does not consider rebalancing.
 */
public class CrashedDeskConsumer extends AbstractInteractiveShutdownConsumer {

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "crashed-desks-simple");

		// Kafka will auto-commit every 5s based on the last poll() call
		props.put("enable.auto.commit", "true");

		Map<Integer, Instant> lastHeartbeat = new TreeMap<>();

		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Collections.singletonList(AirportProducer.TOPIC_STATUS));

			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(5));

				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					lastHeartbeat.put(record.key(), Instant.ofEpochMilli(record.timestamp()));
				}

				// If we sped up the simulation 10x, we should be getting status events every 6 seconds
				// We double that so we are sure we definitely missed a status update.
				final Instant oneMinuteAgo = Instant.now().minusSeconds(12);

				int i = 1;
				System.out.printf("Possibly down terminals as of %s:%n", Instant.now());
				for (Entry<Integer, Instant> entry : lastHeartbeat.entrySet()) {
					if (entry.getValue().isBefore(oneMinuteAgo)) {
						System.out.printf("%3d. %d (no status heartbeat since %s)%n", i++, entry.getKey(), entry.getValue());
					}
				}
				System.out.println();
			}
		}
	}

	public static void main(String[] args) {
		new CrashedDeskConsumer().runUntilEnterIsPressed(System.in);
	}

}
