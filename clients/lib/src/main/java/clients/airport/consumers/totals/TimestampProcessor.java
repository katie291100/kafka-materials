package clients.airport.consumers.totals;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

public class TimestampProcessor<K, V> extends ContextualFixedKeyProcessor<K, V, Long> {

	@Override
	public void process(FixedKeyRecord<K, V> record) {
		// TODO Auto-generated method stub
		String topic = context().recordMetadata().get().topic();
	    String recordDate =
	        Instant.ofEpochMilli(record.timestamp())
	            .atZone(ZoneOffset.UTC)
	            .format(DateTimeFormatter.BASIC_ISO_DATE);
	    context().forward(record.withValue(recordDate));	}

}
