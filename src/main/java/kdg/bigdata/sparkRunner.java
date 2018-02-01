package kdg.bigdata; /**
 * Created by overvelj on 5/07/2016.
 */

public class sparkRunner {


    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.err.println("Usage: kdg.bigdata.sparkRunner.java input_location {output_location}");
            System.exit(1);
        } else {

        }
        TutorialFunctions tf = null;
        if (args.length==3 && args[2].equals("forceLocal")){
             tf = new TutorialFunctions(true);
        } else {
             tf = new TutorialFunctions(false);
        }
        tf.WordCount(args[0],args[1]);
        //tf.ClosureDemo();
        //tf.PrintValues();
        //tf.ElementsPerPartition();
        tf.keepRunning();
        }
    }

