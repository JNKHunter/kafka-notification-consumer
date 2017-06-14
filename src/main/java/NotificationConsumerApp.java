import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by John on 6/14/17.
 */
public class NotificationConsumerApp {
    public static void main(String[] args) {
        int numConsumers = 1;
        String groupId = "notifications";
        List<String> topics = Arrays.asList("notifications");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<ConsumerLoop> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            ConsumerLoop consumer = new ConsumerLoop(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (ConsumerLoop consumer : consumers) {
                    consumer.shutdown();
                }

                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }
}
