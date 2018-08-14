package utility;

import org.apache.spark.sql.SparkSession;

public class InstanceSparkSession {

    // singleton access for InstanceSparkContext
    private static SparkSession instance = SparkSession
            .builder()
            .master("local[*]")
            .appName("creditscoring")
            .getOrCreate();

    private InstanceSparkSession(){};

    public static SparkSession getInstance(){
        // create spark session
        return instance;
    };

};
