package kdg.bigdata;

import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;


import java.util.*;

/**
 * Created by overvelj on 11/07/2016.
 */
public class TutorialFunctions {

    SparkConf conf;
    JavaSparkContext sc;

    public TutorialFunctions(boolean forceLocal) {
        conf = new SparkConf().setAppName("SparkTutorial");
        if (forceLocal) {
            conf.setMaster("local");
        }
        sc = new JavaSparkContext(conf);

    }

    private static final Logger LOGGER = Logger.getLogger(sparkRunner.class);

    public void WordCount(String input, String output) {
       JavaRDD<String> lines = sc.textFile(input,3);
       JavaRDD<String> words = lines.flatMap(l -> IteratorUtils.arrayIterator(l.split(" ")));
       JavaRDD<String> cleanedWords = words.map(w -> w.replaceAll("[^a-zA-Z ]", "")).map(w -> w.toLowerCase());
        JavaPairRDD<String, Integer> pairs = cleanedWords
                .mapToPair(w -> new Tuple2<String, Integer>(w, 1))
                .reduceByKey((a, b) -> a + b);
        JavaRDD<String> pairsString = pairs.map(a -> a._1    + "," + a._2);
        pairsString.saveAsTextFile(output);

        //pairsString.collect().forEach(w -> System.out.println(w));
    }

    //Closure principle
    // Spark maakt taken aan om delen van RDDs parallel te verwerken. Elke taak heeft een closure. Dat zijn de variabelen en methoden waar die taak zicht op heeft.
    // In het onderstaande voorbeeld wordt er telkens een copie  (dus geen pointer naar) van de counter doorgegeven. Dat kan ook moeilijk anders want de verschillende
    // Elke  executor (node) krijgt dus een eigen kopie en als we het op de driver node achteraf willen afdrukken dan zal de waarde van de lokale kopie worden afgedrukt (en daar is nog niets mee gebeurd).
    public void ClosureDemo() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        final int[] counter = {0};
        JavaRDD<Integer> rdd = sc.parallelize(data, 2);

        // Wrong: Don't do this!!
        rdd.foreach(x -> counter[0] += x);

        System.out.println("Counter value: " + counter[0]);
    }

    public void PrintValues() {
        List<Integer> data = Arrays.asList(5, 3, 8, 4, 5);
        JavaRDD<Integer> rdd = sc.parallelize(data, 2);

        //Printen doe je niet zo...
        rdd.foreach(x -> System.out.println("Niet zo" + x));
        // Het probleem hiervan is dat dit wel werkt als je alles lokaal draait. Draai je de job op een cluster met meerdere nodes, dan zal
        // de println worden gestuurd naar de consoleoutput van de node die de deeltaak aan het uitvoeren is.

        // Maar zo...
        // Hier zal de driver alle gegevens verzamelen en dan lokaal afdrukken.
        rdd.collect().forEach(x -> System.out.println("Maar zo" + x));
        // Als het te veel data betreft doe je beter hetvolgende:
        rdd.take(100).forEach(x -> System.out.println("of zo" + x));
    }

    public void ElementsPerPartition() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        // We maken een RDD aan met 5 elementen in twee partities
        JavaRDD<Integer> distData = sc.parallelize(data, 2);
        //distData.cache();
        // Als test voeren we een count actie uit (we tellen gewoon hoeveel elementen er in de RDD zijn)
        System.out.println("Aantal data-elementen: " + distData.count());

        //Om te weten te komen hoeveel elemenenten er per partitie zijn voeren we een mapPartitions uit. mapPartitions heeft als inputparameter een ieterator van alle elementen binnen de partitie.
        JavaRDD<Integer> countElementsPerPartition = distData.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) (Iterator<Integer> input) -> {
            Integer count = 0;
            while (input.hasNext()) {
                //We tellen het aantal elementen via de iterator.
                count++;
                input.next();
            }
            // een mapPartitions functie heeft 0 of meerdere outputrecords. Daarom moeten we het ene resultaat dat we per partitie hebben toch wegschrijven naar een ArrayList.
            ArrayList<Integer> ret = new ArrayList<Integer>();
            ret.add(count);
            return ret.iterator();
        });
        // Per partitie wordt het aantal afgedrukt.
        int index = 0;
        countElementsPerPartition.collect().forEach(x -> System.out.println("Aantal elementen in partitie: " + x));


    }


    public void keepRunning() {
        //Onderstaande loop zorgt ervoor dat de job blijft draaien, zodat je de SparkUI kan inspecteren.
        // Zet deze in comments als je hem niet nodig hebt.
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
        JavaRDD<Integer> distData = sc.parallelize(data, 2);
        Iterator<Integer> it = distData.toLocalIterator();
        while (it.hasNext()) {
            System.out.print("");
        }
    }

}



