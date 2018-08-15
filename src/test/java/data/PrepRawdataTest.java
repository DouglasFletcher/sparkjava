package data;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import utility.InstanceSparkSession;
import utility.ProjectStaticVars;

import javax.inject.Inject;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrepRawdataTest {

    @Inject
    PrepRawdata prepRawdata;

    @Inject
    ProjectStaticVars projectStaticVars;

    @Inject
    InstanceSparkSession instanceSparkSession;

    @Test
    public void createRandForestData_shouldSucceed(){//

        // expected
        int rowCount = 150000;

        // test data
        SparkSession spark = instanceSparkSession.getInstance();
        String fileIn = projectStaticVars.createFileLoc("/01_data/cstraining_kaggle.csv");
        ReadDataset dataset = new ReadDataset(spark, fileIn);
        dataset.readRawData();

        // tests method: rows check
        assertFalse(prepRawdata.createRandForestData(dataset.getPreppedData()).isEmpty());
        assertEquals(rowCount, dataset.getPreppedData().count());
    }


}
