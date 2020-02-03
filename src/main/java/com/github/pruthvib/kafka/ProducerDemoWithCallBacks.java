package com.github.pruthvib.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBacks {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallBacks.class);

		String bootstrapservers = "127.0.0.1:9092";

		// Create Producer Properties
		Properties properties = new Properties();
		// properties.setProperty("bootstrap-servers", bootstrapservers);
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// Create Producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

		for (int i = 0; i < 10; i++) {
			// Create a Producer Record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("kafka-demo",
					"Hello Kafka!!" + Integer.toString(i));

			// Send the Record
			producer.send(record, new Callback() {

				public void onCompletion(RecordMetadata metadata, Exception exception) {
					// Executes everytime when a record is sent successfully or an exception occurs.

					if (exception == null) {
						// Record send successfully
						logger.info("Received new metadata \n" + "Topic: " + metadata.topic() + "\n" + "Partition: "
								+ metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n" + "Timestamp: "
								+ metadata.timestamp());

					} else {
						// exception.printStackTrace();
						logger.error("Error while sending---> ", exception);
					}

				}
			});
		}

		// Flush Data
		producer.flush();

		// Flush and Close
		producer.close();

	}

}
