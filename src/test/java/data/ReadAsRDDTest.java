package data;

import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import utility.InstanceSparkSession;
import utility.ProjectStaticVars;

import javax.inject.Inject;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReadAsRDDTest {

    private ReadAsRDD dataset;

    private SparkSession spark;

    @Inject
    ProjectStaticVars projectStaticVars;

    @Inject
    InstanceSparkSession instanceSparkSession;

    @Before
    public void setUp() throws Exception {
        // test data
        spark = instanceSparkSession.getInstance();
        String fileIn = projectStaticVars.createFileLoc("/01_data/cstraining_kaggle.csv");
        dataset = new ReadAsRDD(spark, fileIn);
    }

    @After
    public void tearDown() throws Exception {
        //
    }

    @Test
    public void readRawData_shouldSucceed(){//
        // expected
        int rowCount = 150000;
        // test method
        dataset.readRawData();
        assertEquals(rowCount, dataset.getRawData().count());
    }

}
