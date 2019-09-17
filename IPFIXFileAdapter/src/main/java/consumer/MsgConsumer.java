package consumer;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import utils.JsonUtils;

public class MsgConsumer {
    private static final String WELCOME_MSG = "Creating consumer thread";
    private String m_bootstrapServer;
    private String m_groupId;
    private String m_topic;
    private String m_prefixFileName = "/home/test/";
    
    private class ConsumerRunnable implements Runnable {
        private CountDownLatch mLatch;
        private KafkaConsumer<String, String> mConsumer;
        
        ConsumerRunnable(String bootStrapServer, String groupId, String topic, CountDownLatch latch) {
            mLatch = latch;
            Properties props = consumerProps(bootStrapServer, groupId);
            mConsumer = new KafkaConsumer<>(props);
            mConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {
                do {
                    ConsumerRecords<String, String> records = mConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                        Timestamp ts = new Timestamp(record.timestamp());
                        String[] value = JsonUtils.IN.getArrayDeviceNameAndComponentValue(record.value());
                        String device = value[0];
                        String component = value[1];
                        String fileName = m_prefixFileName + JsonUtils.IN.getDeviceName(device);
                        fileName += "_" + record.offset();
                        JsonUtils.IN.convertAllStringToFile(fileName, ts, device, component);
                        System.out.println(String.format("Converted the message with offset=%s to %s file.", record.offset(), fileName));
                        } catch (Exception e) {
                            System.out.println(String.format("ERROR: Could not convert the message with offset=%s ", record.offset()));
                        }
                    }
                } while (true);
            } catch (Exception e) {
                System.out.println("ERROR: Something wrong with message " + e.getMessage());
            } finally {
                mConsumer.close();
                mLatch.countDown();
            }
            
        }
        
    }
    
    public MsgConsumer(String bootstrapServer, String goupId, String topic) {
        m_bootstrapServer = bootstrapServer;
        m_groupId = goupId;
        m_topic = topic;
    }
    
    private Properties consumerProps(String bootstrapServer, String groupId) {
        String deserializer = StringDeserializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
    
    /**
     * Creating consumer thread to receive a message from producer
     */
    public void run() {
        System.out.println(WELCOME_MSG);
        CountDownLatch latch = new CountDownLatch(1);
        ConsumerRunnable consumerRunnable = new ConsumerRunnable(m_bootstrapServer, m_groupId, m_topic, latch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();
        
    }
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String server = "192.168.95.7:9092";
        String groupId = "interview";
        String topic = "IPFIX_DATA";
        
        new MsgConsumer(server, groupId, topic).run();
    }
}

