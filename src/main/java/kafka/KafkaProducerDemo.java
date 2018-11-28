package kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka生产者
 *
 * @author ss
 */
public class KafkaProducerDemo {
    private final static String TOPIC = "SmartCommunity-Topic";

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.0.120:9092,10.0.0.153:9092,10.0.0.184:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        producer.send(new ProducerRecord<>(TOPIC, "hello, hello"));

        producer.close();
    }
}
