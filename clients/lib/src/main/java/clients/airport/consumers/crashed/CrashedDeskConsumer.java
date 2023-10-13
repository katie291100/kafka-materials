package clients.airport.consumers.crashed;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import clients.airport.AirportProducer;
import clients.airport.AirportProducer.TerminalInfo;
import clients.airport.AirportProducer.TerminalInfoDeserializer;
import clients.airport.consumers.AbstractInteractiveShutdownConsumer;
import clients.messages.MessageProducer;

/**
 * Naive version of a 'possibly down' consumer, which does not consider
 * rebalancing.
 */
public class CrashedDeskConsumer extends AbstractInteractiveShutdownConsumer implements ConsumerRebalanceListener {

	Table<Integer, Integer, Instant> lastHeartbeat = HashBasedTable.create();
	private KafkaConsumer<Integer, TerminalInfo> consumer ;
	
	public void run() {
		Properties props = new Properties();
		props.put("bootstrap.servers", MessageProducer.BOOTSTRAP_SERVERS);
		props.put("group.id", "crashed-desks-simple");

		// Kafka will auto-commit every 5s based on the last poll() call
		props.put("enable.auto.commit", "true");
		KafkaConsumer<Integer, TerminalInfo> consumer = new KafkaConsumer<>(props, new IntegerDeserializer(), new TerminalInfoDeserializer());

	
		consumer.subscribe(Collections.singletonList(AirportProducer.TOPIC_STATUS), this);

		while (!done) {
			ConsumerRecords<Integer, TerminalInfo> records = consumer.poll(Duration.ofSeconds(5));

			for (ConsumerRecord<Integer, TerminalInfo> record : records) {
				lastHeartbeat.put(record.partition(), record.key(), Instant.ofEpochMilli(record.timestamp()));
			}

			// If we sped up the simulation 10x, we should be getting status events every 6
			// seconds
			// We double that so we are sure we definitely missed a status update.
			final Instant oneMinuteAgo = Instant.now().minusSeconds(12);

			int i = 1;
			System.out.printf("Possibly down terminals as of %s:%n", Instant.now());
			for (Table.Cell<Integer, Integer, Instant> cell : lastHeartbeat.cellSet()) {
				int deskId = cell.getColumnKey();
				Instant lastEvent = cell.getValue();
				if (lastEvent.isBefore(oneMinuteAgo)) {
					System.out.printf("%3d. %d (no status heartbeat since %s)%n", i++, deskId, lastEvent);
				}
				
				}
			System.out.println(
	                "lsit partitions"
	                    + consumer.listTopics().toString());
		System.out.println();
		}
	}

	public static void main(String[] args) {
		new CrashedDeskConsumer().runUntilEnterIsPressed(System.in);
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		System.out.println(
                "Lost partitions" + partitions.stream().map(TopicPartition::partition).toList());
        partitions.forEach(partition -> lastHeartbeat.row(partition.partition()).clear());
          		
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		if (consumer!=null) {
			System.out.println(
	                "Gained partitions"
	                    + partitions.stream().map(TopicPartition::partition).toList());
	       consumer.seekToBeginning(partitions);	
		}
	}

}
