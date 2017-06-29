package com.ingestion.realtime;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;

import java.io.IOException; 
import java.io.InputStream; 
import java.util.Arrays; 
import java.util.HashMap; 
import java.util.HashSet; 
import java.util.Map; 
import java.util.Properties; 
import java.util.Set; 
import java.util.stream.Collectors; 
 
import kafka.serializer.StringDecoder; 
 
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory;

import com.google.common.io.Resources;

import org.apache.spark.SparkConf; 
import org.apache.spark.streaming.api.java.*; 
import org.apache.spark.streaming.kafka010.KafkaUtils; 
import org.apache.spark.streaming.Durations; 
 
 
/**
 * Read data from Kafka data stream 
 * @author tsehsun 
 * 
 */ 
public class DataStreamReader { 
 private final static Logger logger = LoggerFactory 
   .getLogger(DataStreamReader.class); 
 private JavaStreamingContext jssc; 
 private SparkConf conf; 
 private Properties properties; 
 private Set<String> topicSet; 
 private Map<String, Object> kafkaParams; 
 
 public DataStreamReader() { 
 
  readPropertiesFile(); 
  long interval = Long 
    .parseLong(properties.getProperty("batch.interval")); 
  String appName = properties.getProperty("app.name"); 
  String master = properties.getProperty("master"); 
   
  conf = new SparkConf().setAppName(appName); 
  conf.setMaster(master); 
  jssc = new JavaStreamingContext(conf, Durations.seconds(interval)); 
 
  // get the list of topics, and do a little post processing 
  topicSet = new HashSet<String>(Arrays.asList(properties.getProperty( 
    "topics").split(","))); 
  topicSet = topicSet.stream().map(String::trim) 
    .collect(Collectors.toSet()); 
 
  String brokers = properties.getProperty("metadata.broker.list"); 
 
  kafkaParams.put("bootstrap.servers", "localhost:9092");
  kafkaParams.put("key.deserializer", StringDeserializer.class);
  kafkaParams.put("value.deserializer", StringDeserializer.class);
  //kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
  kafkaParams.put("auto.offset.reset", "latest");
  kafkaParams.put("enable.auto.commit", true); 
 
  logger.info( 
    "DataStreamReader properties: appName:{}, master:{}, interval:{}, topicSet:{}, brokers:{}", 
    appName, master, interval, topicSet, brokers); 
 } 
 
 private void readPropertiesFile() { 
  properties = new Properties(); 
  ClassLoader loader = Thread.currentThread().getContextClassLoader(); 
  String resourceName = "consumer.props"; 
  try (InputStream resourceStream = loader 
    .getResourceAsStream(resourceName)) { 
   properties.load(resourceStream); 
  } catch (IOException e) { 
   logger.error("Cannot load property file {}", resourceName, e); 
  } 
  
 

 } 
 
 public void startReadingDataStream(int i) { 
  // first parameter is Kafka topic, and second is content (in this case, 
  // a line) 
	 Collection<String> topics = Arrays.asList("fast-messages");
	  JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(
			    jssc,
			    LocationStrategies.PreferConsistent(),
			    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
			  );
   
  //processor.process(messages); 
	  messages.mapToPair(
			  new PairFunction<ConsumerRecord<String, String>, String, String>() {
				    @Override
				    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
				      return new Tuple2<>(record.key(), record.value());
				    }
				  });
  // start the computation 
  jssc.start(); 
  jssc.awaitTermination(); 
   
 } 
 public static void main(String[] args) {
	new DataStreamReader().startReadingDataStream(0);
}
}
