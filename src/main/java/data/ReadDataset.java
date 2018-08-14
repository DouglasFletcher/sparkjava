package data;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;


public class ReadDataset {

    private SparkSession spark;
    private String fileIn;
    private JavaRDD<String[]> readData;

    public ReadDataset(SparkSession spark, String fileIn){
        this.spark = spark;
        this.fileIn = fileIn;
    }

    public void readRawData(){
        // read CSV data
        JavaRDD<String> rawData = spark.read()
                .option("header","true")
                .textFile(fileIn)
                .javaRDD();
        // remove header
        String headerVal = rawData.first();
        JavaRDD<String> rawDataFilter = rawData.filter(s->!s.contains(headerVal));
        // create row vector object
        JavaRDD<String[]> rawDataRow = rawDataFilter.map(s->s.split(","));
        this.readData = rawDataRow;
    }

    public JavaRDD<String[]> getPreppedData() {
        return readData;
    }


}
