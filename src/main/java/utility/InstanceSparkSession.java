package utility;

import base.config.SJConfig;
import org.apache.spark.sql.SparkSession;

public class InstanceSparkSession {

    // singleton access for InstanceSparkContext
    private static SparkSession instance = SparkSession
            .builder()
            .master(SJConfig.sparkSessionPropMaster.getValue())
            .appName(SJConfig.sparkSessionPropAppname.getValue())
            .getOrCreate();

    private InstanceSparkSession(){};

    public static SparkSession getInstance(){
        // create spark session
        return instance;
    };

};
