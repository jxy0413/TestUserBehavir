package cn.bjfu.hotiems;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * Created by jxy on 2021/4/15 0015 18:06
 */
public class KafkaProducerUtil {
    public static void main(String[] args)throws Exception {
        writeToKafka("hostitems");
    }

    //包装写一个kafka的方法
    public static void writeToKafka(String topic)throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "Master:9092,Worker1:9092,Worker3:9092,Worker4:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //定义一个Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);

        BufferedReader bufferedReader = new BufferedReader(new FileReader("F:\\\\wsy\\\\1.csv"));
        String line;
        while ((line = bufferedReader.readLine())!=null){
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();
    }
}
