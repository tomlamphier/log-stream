/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datasciex;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import java.util.List;

import redis.clients.jedis.Jedis; 

public final class LogStream {
  private LogStream() {
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: LogStream <host> <port> <path-to-jar");
      System.exit(1);
    }


    Jedis jedis = new Jedis("localhost"); 
    System.out.println("Connection to server sucessfully"); 
    //check whether server is running or not 
    System.out.println("Server is running: "+jedis.ping()); 

    jedis.set("tutorial-name", "Redis tutorial");
    System.out.println("Stored string in Redis: " + jedis.get("tutorial-name"));

    String host = args[0];
    int port = Integer.parseInt(args[1]);
    String pathToJar = args[2];

    Duration batchInterval = new Duration(60000);
    SparkConf sparkConf = new SparkConf().setAppName("LogStream")
        .setMaster("local[2]");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, batchInterval);
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
      FlumeUtils.createStream(ssc, host, port);

    JavaDStream<SparkFlumeEvent> windowFlume = flumeStream.window(new Duration(300000), new Duration(60000));
    JavaDStream<String> windowLines = windowFlume.map(e ->  new String(e.event().getBody().array()));

 // Convert RDDs of the words DStream to DataFrame and run SQL query
    windowLines.foreachRDD((rdd, time) -> {
    SparkSession spark = JavaSparkSessionSingleton.getInstance(rdd.context().getConf(), pathToJar);
      // Convert JavaRDD[String] to JavaRDD[bean class] to DataFrame
      JavaRDD<JavaRecord> rowRDD = rdd.map(line -> {
        JavaRecord record = new JavaRecord();
        record.init(line);
        return record;
      });
      Dataset<Row> linesDataFrame = spark.createDataFrame(rowRDD, JavaRecord.class);

      // Creates a temporary view using the DataFrame
      linesDataFrame.createOrReplaceTempView("lines");

      // Do word count on table using SQL and print it
      Dataset<Row> lineCountsDataFrame =
          spark.sql("select ip, requestUri, count(*) as ctr from lines group by ip, requestUri having count(*) > 5");
      System.out.println("A========= " + time + "=========A");
      lineCountsDataFrame.show();
     
      List<Row> rows = lineCountsDataFrame.collectAsList();
      System.out.println("list length = " + rows.size());
      System.out.println("B========= " + time + "=========B");

      for (Row r: rows) {
          //System.out.println("row contents: " + r.getString(0));
          String ip_key = r.getString(0);
          if (!jedis.exists(ip_key)) {
              jedis.set(ip_key, "T");
              jedis.expire(ip_key, 120);
          }
      }
 
      System.out.flush();
    });


    ssc.start();
    ssc.awaitTermination();
  }
}

/** Lazily instantiated singleton instance of SparkSession */
class JavaSparkSessionSingleton {
  private static transient SparkSession instance = null;
  public static SparkSession getInstance(SparkConf sparkConf, String pathToJar) {
    if (instance == null) {
      instance = SparkSession
        .builder()
        //.config(sparkConf)
        .appName("LogStream")
        .config("spark.jars", pathToJar)
        .getOrCreate();
    }
    return instance;
  }
}

