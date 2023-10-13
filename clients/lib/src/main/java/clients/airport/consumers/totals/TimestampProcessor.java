package clients.airport.consumers.totals;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.streams.processor.api.ContextualFixedKeyProcessor;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.processor.internals.AbstractProcessorContext;

import clients.airport.AirportProducer.TerminalInfo;

public class TimestampProcessor extends ContextualProcessor<Integer, TerminalInfo, String, Long> {
  @Override
  public void process(Record <Integer, TerminalInfo> record) {
    String recordDate =
        Instant.ofEpochMilli(record.timestamp())
            .atZone(ZoneOffset.UTC)
            .format(DateTimeFormatter.BASIC_ISO_DATE);
    context().forward(new Record<String, Long>(recordDate, record.timestamp(), record.timestamp()));
  }
}