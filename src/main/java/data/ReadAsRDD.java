package data;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;


public class ReadAsRDD {

    private SparkSession spark;
    private String fileIn;
    private JavaRDD<String[]> rawData;

    /**
     * <h3>Constuctor</h3>
     * <p>Read dataset constructor</p>
     * @param spark SparkSession: spark session
     * @param fileIn String: file location
     */
    public ReadAsRDD(SparkSession spark, String fileIn){
        this.spark = spark;
        this.fileIn = fileIn;
    }

    /**
     * <h3>read data</h3>
     * <p>read data as JavaRDD<String></p>
     */
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
        this.rawData = rawDataRow;
    }

    /**
     * <h3>get data</h3>
     * <p>get raw data</p>
     * @return rawData Dataset<Row>: output data
     */
    public JavaRDD<String[]> getRawData() {//
        return rawData;//
    }


}
