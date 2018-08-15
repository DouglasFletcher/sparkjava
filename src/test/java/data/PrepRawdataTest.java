package data;

import base.config.SJConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utility.InstanceSparkSession;
import utility.ProjectStaticVars;

import javax.inject.Inject;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PrepRawdataTest {

    private ReadDataset dataset;

    private SparkSession spark;

    @Inject
    PrepRawdata prepRawdata;

    @Inject
    ProjectStaticVars projectStaticVars;

    @Inject
    InstanceSparkSession instanceSparkSession;

    @Before
    public void setUp() throws Exception {
        // test data
        spark = instanceSparkSession.getInstance();
        String fileIn = projectStaticVars.createFileLoc("/01_data/cstraining_kaggle.csv");
        dataset = new ReadDataset(spark, fileIn);
    }

    @After
    public void tearDown() throws Exception {
        //
    }

    @Test
    public void createRandForestData_shouldSucceed(){//
        // expected
        int rowCount = 150000;
        dataset.readRawData();
        // tests method: rows check
        assertFalse(prepRawdata.createRandForestData(dataset.getPreppedData()).isEmpty());
        assertEquals(rowCount, dataset.getPreppedData().count());
    }


}
