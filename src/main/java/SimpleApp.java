import data.PrepRawdataDataset;
import data.PrepRawdataRdd;
import data.ReadAsDataset;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import utility.InstanceSparkSession;
import utility.ProjectStaticVars;

import javax.inject.Inject;

public class SimpleApp {

    @Inject
    PrepRawdataRdd prepRawdataRdd;

    public static void main(String[] args) {

        System.setProperty("HADOOP_HOME", "C:\\winutil\\");

        // create spark session
        SparkSession spark = InstanceSparkSession.getInstance();
        String fileIn = ProjectStaticVars.createFileLoc("/01_data/cstraining_kaggle.csv");

        // read raw data
        Dataset<Row> dataset = new ReadAsDataset(spark, fileIn).getRawData();

        // transform data
        Dataset<Row> dataTrans = new PrepRawdataDataset(dataset).getDatasetOut();

        spark.stop();

    }
};


