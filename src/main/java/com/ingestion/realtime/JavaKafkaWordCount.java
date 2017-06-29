package com.ingestion.realtime;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import com.google.common.collect.Lists;

import scala.Tuple2;

/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: JavaKafkaWordCount <master> <zkQuorum> <group> <topics> <numThreads>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `./bin/run-example org.apache.spark.streaming.examples.JavaKafkaWordCount local[2] zoo01,zoo02,
 *    zoo03 my-consumer-group topic1,topic2 1`
 */

public final class JavaKafkaWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");

  private JavaKafkaWordCount() {
  }

  public static void main(String[] args) {
	  Logger.getLogger("org").setLevel(Level.ERROR);
   
    //StreamingExamples.setStreamingLogLevels();

    // Create the context with a 1 second batch size
	  SparkConf _sparkConf = new SparkConf().set(
				"spark.streaming.receiver.writeAheadLog.enable", "false").setMaster("local[2]").setAppName("SOME APP NAME").set("spark.ui.port", "4041");


		int numberOfReceivers = 3;
		
		
		JavaStreamingContext ssc = new JavaStreamingContext(_sparkConf, new Duration(1000));
		
    

    Map<String, Integer> topicMap = new HashMap<String, Integer>();
   
    topicMap.put("fast-messages", 2);

    JavaPairDStream<String, String> messages = KafkaUtils.createStream(ssc, "localhost:2181", "test-consumer-group", topicMap);

    JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
     // @Override
      public String call(Tuple2<String, String> tuple2) {
        return tuple2._2();
      }
    });
    System.out.println("#################");
    lines.dstream().print();
    JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
    //  @Override
      public Iterable<String> call(String x) {
        return Lists.newArrayList(SPACE.split(x));
      }
    });

    words.count();
    /*JavaPairDStream<String, Integer> wordCounts = words.map(
      new PairFunction<String, String, Integer>() {
       // @Override
        public Tuple2<String, Integer> call(String s) {
          return new Tuple2<String, Integer>(s, 1);
        }
      }).reduceByKey(new Function2<Integer, Integer, Integer>() {
        //@Override
        public Integer call(Integer i1, Integer i2) {
          return i1 + i2;
        }
      });

    wordCounts.print();*/
    ssc.start();
    ssc.awaitTermination();
  }
}
