package clients.airport.consumers.windows;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
 * Computes windowed checkins over each general area of the airport (defined
 * as the hundreds digit of the terminal ID).
 *
 * This version doesn't handle rebalances, and it autocommits so it won't maintain the time window properly upon rebalancing.
 */
public class WindowedAreaCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

	private Duration windowSize = Duration.ofSeconds(30);

	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "windowed-area-stats");
		props.put("enable.auto.commit", "true");

		Map<Integer, TimestampSlidingWindow> windowCheckinsByArea = new HashMap<>();

		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
			consumer.subscribe(Arrays.asList(AirportProducer.TOPIC_CHECKIN));

			Instant streamTime = null;
			while (!done) {
				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(5));
				if (records.isEmpty()) continue;

				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
					Instant recordInstant = Instant.ofEpochMilli(record.timestamp());
					if (streamTime == null || recordInstant.isAfter(streamTime)) {
						streamTime = recordInstant;
					}

					windowCheckinsByArea
						.computeIfAbsent(record.key() / 100, key -> new TimestampSlidingWindow())
						.add(Instant.ofEpochMilli(record.timestamp()));
				}

				// We always have to look *back*, as that's when the window is complete
				Instant windowStart = streamTime.minus(windowSize);
				for (Map.Entry<Integer, TimestampSlidingWindow> entry : windowCheckinsByArea.entrySet()) {
					System.out.printf("Number of checkins in area %d between %s and %s: %d%n", entry.getKey(),
							windowStart, streamTime, entry.getValue().windowCount(windowStart, streamTime));
				}
			}
		}
	}

	public static void main(String[] args) {
		new WindowedAreaCheckinsConsumer().runUntilEnterIsPressed(System.in);
	}

}
