package kdg.bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StreamTutorialFunctions {

    SparkConf conf;
    JavaStreamingContext ssc;



    public StreamTutorialFunctions(boolean forceLocal, int batchDuration) {
        conf = new SparkConf().setAppName("SparkStreamingTutorial");
        // De code is er op voorzien dat als je lokaal werkt een pseudocluster gebruikt. Daarvoor moet je de master op local[2] instellen.
        // Als je de gecompileerde code  op een productieve omgeving zoals AWS of clouderaVM wil uitvoeren wordt de master default ingesteld door die omgeving.
        // Daarom geven we geen setMaster mee als we niet lokaal draaien.
        if (forceLocal) {
            conf.setMaster("local[2]");
        }
        ssc = new JavaStreamingContext(conf, new Duration(batchDuration));
    }

    public void streamWordCount(String host, String port, String output) {

    }

    public void streamWordCountWithState(String host, int port, String op) throws Exception {


    }


}
