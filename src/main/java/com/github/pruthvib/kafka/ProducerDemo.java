package com.github.pruthvib.kafka;

//Dummy Comment
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;


public class ProducerDemo {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		String bootstrapservers = "127.0.0.1:9092";

		//Create Producer Properties
		Properties properties = new Properties();
		//properties.setProperty("bootstrap-servers", bootstrapservers);
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		
		//Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		
		//Create a Producer Record
		ProducerRecord<String, String> record = new ProducerRecord<String, String> ("kafka-demo", randomStringGen.generateRandomString(5)); 
		
		
		
		//Send the Record
		producer.send(record);
		
		//Flush Data
		producer.flush();
		
		//Flush and Close
		producer.close();
		
		
	}

}
