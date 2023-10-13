package clients.airport.consumers.totals;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Simple consumer which just counts totals over checkins. This is very simplistic: it doesn't
 * handle rebalancing, and wouldn't scale as it doesn't apply any windows or splits the input
 * in any particular way.
 */
public class TotalCheckinsConsumer extends AbstractInteractiveShutdownConsumer {

	public void run() {
		// 1. Use StreamsBuilder to build the topology
		StreamsBuilder builder = new StreamsBuilder();

		// 2. Configure the Properties (need bootstrap serves and app ID at least)
		Properties props = new Properties();
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AirportProducer.BOOTSTRAP_SERVERS);
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-status");
		
		int started = 0, completed = 0, cancelled = 0;

		// Consume the status updates from the TOPIC_STATUS (Integer key and TerminalInfo value)
		Serde<TerminalInfo> serde = new AirportProducer.TerminalInfoSerde();
		builder.stream(List.of(
                AirportProducer.TOPIC_COMPLETED,
                AirportProducer.TOPIC_CANCELLED,
                AirportProducer.TOPIC_CHECKIN), Consumed.with(Serdes.Integer(), serde)).processValues(TimestampProcessor<Integer, AirportProducer.TerminalInfo>::new)
			// Map each key-value to a formatted string
			.mapValues((k, v) -> String.format("Status from %s: %d sheets%n", k, v.paperLeft))
			// Print them out to the standard output
			.print(Printed.toSysOut());
		
//
//		try (KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer())) {
//			consumer.subscribe(Arrays.asList(AirportProducer.TOPIC_CHECKIN, AirportProducer.TOPIC_COMPLETED, AirportProducer.TOPIC_CANCELLED));
//
//			while (!done) {
//				ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(1));
//				if (records.isEmpty()) continue;
//
//				Instant latestInstant = null;
//				for (ConsumerRecord<Integer, TerminalInfo> record : records) {
//					Instant recordTime = Instant.ofEpochMilli(record.timestamp());
//					if (latestInstant == null || latestInstant.isBefore(recordTime)) {
//						latestInstant = recordTime;
//					}
//
//					switch (record.topic()) {
//					case AirportProducer.TOPIC_CHECKIN:
//						++started; break;
//					case AirportProducer.TOPIC_COMPLETED:
//						++completed; break;
//					case AirportProducer.TOPIC_CANCELLED:
//						++cancelled; break;
//					}
//				}
//
//				System.out.printf("Checkins at %s: %d started, %d completed, %d cancelled%n", latestInstant, started, completed, cancelled);
//			}
//		}
	}

	public static void main(String[] args) {
		new TotalCheckinsConsumer().runUntilEnterIsPressed(System.in);
	}

}

