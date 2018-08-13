import java.text.SimpleDateFormat;
import java.util.Calendar;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {

    public static void main(String[] args) {

        System.setProperty("HADOOP_HOME", "C:\\winutil\\");

        // timer
        String startTime = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());
        long startDiff = System.currentTimeMillis();

        // ********************
        // create spark session
        // ********************
        SparkSession spark = InstanceSparkSession.getInstance();
        String proloc = ProjectStaticVars.getProjloc();
        String fileIn = ProjectStaticVars.createFileLoc("/01_data/cstraining_kaggle.csv");

        // *************
        // read raw data
        // *************
        ReadDataset dataset = new ReadDataset(spark, fileIn);
        dataset.readRawData();

        // *************************
        // prep data transformations
        // *************************
        JavaRDD<LabeledPoint> datasetRandomForest = PrepRawdata.createRandForestData(
                dataset.getPreppedData()
        );
        System.out.println(datasetRandomForest.first());

        // *********************************
        // create Random Forest data / model
        // *********************************
        //CreateRandForestModel.createRandForestModel(datasetRandomForest, spark, proloc);

        // *****************
        // end spark session
        // *****************
        spark.stop();

        // end timer
        String endTime = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

        long endDiff = System.currentTimeMillis();
        long totalTime = (endDiff - startDiff) / 1000;

        System.out.println("Start Time : " + startTime);
        System.out.println("End Time : " + endTime);
        System.out.println("Total time taken to run : " + totalTime + " seconds");

    }
};


