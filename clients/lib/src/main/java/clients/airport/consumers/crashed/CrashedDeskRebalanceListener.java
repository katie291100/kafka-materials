package clients.airport.consumers.crashed;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import com.google.common.collect.Table;

public class CrashedDeskRebalanceListener implements ConsumerRebalanceListener{

	 private CrashedDeskConsumer consumer;

	 public CrashedDeskRebalanceListener(CrashedDeskConsumer consumer) {
	        this.consumer = consumer;
	    }
	
	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		// TODO the consumer should get ready to process messages coming from these partitions

        for (TopicPartition tp : partitions) {
        	 for (Table.Cell<Integer, Integer, Instant> cell : consumer.lastHeartbeat.cellSet()) {
                 if (cell.getRowKey() == tp.partition()) {
                	 consumer.lastHeartbeat.remove(cell.getRowKey(), cell.getColumnKey());
                 }
             }    
        	}
		}
		

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		// TODO the consumer should discard any internal state related to these
		// partitions, and commit anything that the new consumer handling this partition would require Map<Integer, Map<Integer, Instant>> Partition ID -> desk ID -> last status
		
	}
	
	
}

