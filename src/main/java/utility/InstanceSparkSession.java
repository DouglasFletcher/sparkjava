package utility;

import base.config.SJConfig;
import org.apache.spark.sql.SparkSession;

/**
 * <h3>Spark Session</h3>
 * <p>Spark instantiation.</p>
 */
public class InstanceSparkSession {

    /**
     * <h3>spark session</h3>
     * <p>create spark session</p>
     */
    private static SparkSession instance = SparkSession
            .builder()
            .master(SJConfig.sparkSessionPropMaster.getValue())
            .appName(SJConfig.sparkSessionPropAppname.getValue())
            .getOrCreate();

    /**
     * <h3>constructor</h3>
     * <p>spark session constructor</p>
     */
    private InstanceSparkSession(){};

    /**
     * <h3>get Spark session instance</h3>
     * @return instance SparkSession
     */
    public static SparkSession getInstance(){
        // create spark session
        return instance;
    };

};
