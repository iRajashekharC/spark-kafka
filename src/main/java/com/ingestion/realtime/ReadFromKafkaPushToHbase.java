package com.ingestion.realtime;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.spark.FamilyHFileWriteOptions;
import org.apache.hadoop.hbase.spark.JavaHBaseContext;
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.*;

/**
 * Created by sunilpatil on 1/11/17.
 */
public class ReadFromKafkaPushToHbase {
	
    public static void main(String[] argv) throws Exception{

        // Configure Spark to connect to Kafka running on local machine
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                        "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                        "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,"test-consumer-group");
        //kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);

        //Configure Spark to listen messages in topic test
        Collection<String> topics = Arrays.asList("test-output");

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkKafka10WordCount").set("spark.driver.allowMultipleContexts", "true");
        //Read messages in batch of 30 seconds
        //JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(2));
        JavaSparkContext ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(conf));
        JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.seconds(2));
        // Start reading messages from Kafka and get DStream
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(jssc, LocationStrategies.PreferConsistent(), 
                                              ConsumerStrategies.<String,String>Subscribe(topics,kafkaParams));

        // Read value of each message from Kafka and return it
        JavaDStream<String> lines = stream.map(new Function<ConsumerRecord<String,String>, String>() {
            //@Override
            public String call(ConsumerRecord<String, String> kafkaRecord) throws Exception {
                return kafkaRecord.value();
            }
        });
        String tableName = "bulkload-table-test";
     // Default HBase configuration for connecting to localhost on default port.
	      Configuration conf1 = HBaseConfiguration.create();
	      // Simple Spark configuration where everything runs in process using 2 worker threads.
	      SparkConf sparkconf = new SparkConf().setAppName("Spark to Hbase").setMaster("local[2]");
	      // The Java Spark context provides Java-friendly interface for working with Spark RDDs
	      JavaSparkContext jsc = new JavaSparkContext(sparkconf);
	      // The HBase context for Spark offers general purpose method for bulk read/write
	      JavaHBaseContext hbaseContext = new JavaHBaseContext(jsc, conf1);
	      // The entry point interface for the Spark SQL processing module. SQL-like data frames
	      // can be created from it.
	      SQLContext sqlContext = new org.apache.spark.sql.SQLContext(jsc);
	      stream.foreachRDD(eachRDD -> {
	    	  hbaseContext.bulkLoad(eachRDD, TableName.valueOf(tableName), null, 
	    			  "/tmp/", new HashMap<byte[], FamilyHFileWriteOptions>(), false, HConstants.DEFAULT_MAX_FILE_SIZE);
	          
	      });
	      jssc.start();
        jssc.awaitTermination();
    }
    public static class BulkLoadFunction implements Function<String, Pair<KeyFamilyQualifier, byte[]>> {

        @Override
        public Pair<KeyFamilyQualifier, byte[]> call(String v1) throws Exception {
          if (v1 == null)
            return null;
          String[] strs = v1.split(",");
          if(strs.length != 4)
            return null;
          KeyFamilyQualifier kfq = new KeyFamilyQualifier(Bytes.toBytes(strs[0]), Bytes.toBytes(strs[1]),
              Bytes.toBytes(strs[2]));
          return new Pair(kfq, Bytes.toBytes(strs[3]));
        }
      }
}
