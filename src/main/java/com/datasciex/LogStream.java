// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
// Program LogStream - read streaming log data and detect brute force 
//                     hacking attempts.
//
// Outline of Processing
// ---------------------
//   1. Get cmd line parameters.  These point to the Flume input stream source.
//   2. Create a Redis client. This will be used to store any malicious IP
//      addresses.  Other applications, notably an nginx server, can use 
//      this to filter requests.
//   3. Set up an input stream that points to the Flume source, convert that
//      to a String format, then set up a sliding window on the stream.  We
//      want to input new data at 1-minute intervals and process with 5 
//      minutes of data since we are looking for a large number of POST's
//      over a short period.
//   4. Set up a processing loop for the windowed data:
//        a.  Translate data from String to Java class.
//        b.  Use Java class RDD to create a dataset.
//        c.  Query dataset for repeated POST attempts from a given IP.
//        d.  Store malicious IP's in Redis db.
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

package com.datasciex;

import org.apache.spark.rdd.RDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.flume.FlumeUtils;
import org.apache.spark.streaming.flume.SparkFlumeEvent;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.storage.StorageLevel;
import java.util.List;
import redis.clients.jedis.Jedis;

public final class LogStream {
  private LogStream() {
  }

  private final static String FILEPATH = "access.log";
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.println("Usage: LogStream <hostname> <port>");
      System.exit(1);
    }

    String host = args[0];
    int port = Integer.parseInt(args[1]);

    Jedis jedis = new Jedis("localhost");
    JavaSparkContext sc = new JavaSparkContext();
    JavaStreamingContext strCxt = new JavaStreamingContext(sc, new Duration(60 * 1000));
    JavaReceiverInputDStream<SparkFlumeEvent> flumeStream =
      FlumeUtils.createStream(strCxt, host, port, StorageLevel.MEMORY_ONLY());
    JavaDStream<String> logLines = flumeStream.map(e -> new String(e.event().getBody().array()));
    JavaDStream<String> logWindow = logLines.window(new Duration(5 * 60 * 1000),
      new Duration(60 * 1000));
    logWindow.foreachRDD((rdd, time) -> {
      if (rdd.count() > 0) {
        JavaRDD<JavaRecord> logRecs = rdd.map(new Function<String, JavaRecord>() {
          public JavaRecord call(String line) {
          JavaRecord record = new JavaRecord();
          record.init(line);
          return record;
          }
        });
        SparkSession ss = SparkSession.builder().getOrCreate();
        Dataset<Row> logDF = ss.createDataFrame(logRecs, JavaRecord.class);
        logDF.createOrReplaceTempView("logrecs");

        Dataset<Row> ipCounts = ss.sql("select ip, requestUri, count(*) as ctr "
          + "from logrecs where requestUri like 'POST%' group by ip, requestUri having count(*) > 5");
        List<Row> rows = ipCounts.collectAsList();
        for (Row r: rows) {
          String ipKey = r.getString(0);
          if (!jedis.exists(ipKey)) {
            jedis.set(ipKey, "T");
          }
        }
    });

    strCxt.start();
    strCxt.awaitTermination();
    sc.stop();
  }
}
