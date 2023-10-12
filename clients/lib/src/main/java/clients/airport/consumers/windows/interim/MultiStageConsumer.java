package clients.airport.consumers.windows.interim;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import clients.airport.consumers.windows.TimestampSlidingWindow;
import clients.messages.MessageProducer;

/**
 * This is the starting code for a multi-stage consumer, which first
 * re-keys checkins by area into an interim topic, and then has another
 * thread consume those events.
 * 
 * Computes windowed checkins over each general area of the airport (defined
 * as the hundreds digit of the terminal ID).
 */
public class MultiStageConsumer {

	private static final String TOPIC_AREA_CHECKIN = "selfservice-area-checkin";
	private static final String GROUP_ID = "windowed-area-multistage";

	private Duration windowSize = Duration.ofSeconds(30);
	private volatile boolean done = false;

	private void runAreaTask() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MessageProducer.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		/*
		 * TODO: consume checkins and produce an event whose key is the area and whose
		 * value is the original record timestamp into the topic referenced by the
		 * TOPIC_AREA_CHECKIN constant.
		 * 
		 * Exit the while loop when the 'done' field becomes true.  
		 */
	}

	private void runWindowTask() {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, MessageProducer.BOOTSTRAP_SERVERS);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

		Map<Integer, Set<Integer>> partitionToAreas = new HashMap<>();
		Map<Integer, TimestampSlidingWindow> areasToWindows = new HashMap<>();

		/*
		 * TODO: consume events from the new interim topic (TOPIC_AREA_CHECKIN) and use
		 * those to compute windowed per-area counts, similarly to our old consumer.
		 *
		 * The next step would be to use a rebalance listener, forgetting old results
		 * and seeking to the beginning when we are assigned new partitions.
		 */
	}

	public void run() throws IOException, InterruptedException {
		Thread tAreaTask = new Thread(this::runAreaTask, "AreaTask");
		Thread tWindowTask = new Thread(this::runWindowTask, "WindowTask");

		tAreaTask.start();
		tWindowTask.start();

		try (BufferedReader bR = new BufferedReader(new InputStreamReader(System.in))) {
			// We can press Enter to exit cleanly
			bR.readLine();
		} finally {
			done = true;
		}

		tAreaTask.join();
		tWindowTask.join();
	}

	public static void main(String[] args) {
		try {
			new MultiStageConsumer().run();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
