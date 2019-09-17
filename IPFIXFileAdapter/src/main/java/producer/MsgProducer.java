package producer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import utils.JsonUtils;

public class MsgProducer {
    private static String m_server = "192.168.95.7:9092";
    private static String m_topic = "IPFIX_DATA";
    private static String m_fileName = "/home/test/test.txt";
    
    private final KafkaProducer<String, String> mProducer;

    private Properties producerProps(String bootstrapServer) {
        String seralier = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, seralier);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, seralier);
        return props;
    }
    
    public MsgProducer(String bootstrapServer) {
        Properties props = producerProps(bootstrapServer);
        mProducer = new KafkaProducer<>(props);

        System.out.println("Producer initialized");
    }
    
    /**
     * @param topic
     * @param key
     * @param value
     * @throws InterruptedException
     * @throws ExecutionException
     * Putting a message value into producer to start sending a message to consumer
     */
    public void put(String topic, String key, String value) throws InterruptedException, ExecutionException {
        System.out.println("Put value: " + value + ", for key: " + key);
        
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        mProducer.send(record, (recordMetadata, e) -> {
            if (e != null) {
                System.out.println("Error while producing" + e);
                return;
            }
            
            System.out.println("Received new data .\n" + 
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset());
        }).get();
    }
    
    public void close() {
        mProducer.close();
        System.out.println("Closed producer's connection");
    }
    
    public static void main(String[] args) throws InterruptedException, ExecutionException, FileNotFoundException {
        FileReader reader = new FileReader(m_fileName);
        
        MsgProducer producer = new MsgProducer(m_server);
        JsonUtils.IN.readFile(m_server, reader, m_topic, producer);
    }
}
