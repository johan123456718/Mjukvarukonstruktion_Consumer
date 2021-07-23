
import interfaces.DbInterface;
import model.JournalRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.time.Duration;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Properties;

//Class for consuming a kafka stream.
public class Consumer {

    private KafkaConsumer<String, String> consumer;
    private Duration duration;
    private Thread workerThread;
    private DbInterface DbManager;

    //Constructor that configures which kafka stream to subscribe to. 
    public Consumer(DbInterface DbManager, String groupID, String ClusterIpAddress, String  clusterPort){
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "HelloConsumer");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterIpAddress + ":" + clusterPort);
        props.put("group.id", groupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        duration = Duration.ofMillis(100);
        consumer = new KafkaConsumer<>(props);
        this.DbManager = DbManager;
    }

    //run on worker thread
    //Consumes messages from the stream corresponding to the one specified in the constructor. 
    //When a record is recieved it is parsed and saved to the database. 
    public void consumeMessages(String topic){
        consumer.subscribe(Arrays.asList(topic));
        workerThread = new Thread(){
            public void run(){
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(duration);
                    //consumer.seek(topicPartition, 100);
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.timestamp(), record.value());
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTimeInMillis(record.timestamp());
                        try{
                            JournalRecord journalRecord = Parser.StringToJournalRecord(record.value());
                            DbManager.saveToDB(journalRecord);
                        }catch (ArrayIndexOutOfBoundsException e){

                        }
                    }
                }
            }
        };
        workerThread.start();
    }

    //Call to stop consuming messages
    public void stopConsume(){
        consumer.unsubscribe();
        workerThread.interrupt();
    }
}
