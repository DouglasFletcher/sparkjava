package modelling;

import data.PrepRawdata;
import data.ReadDataset;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.SparkSession;
import org.apache.zookeeper.Op;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import utility.InstanceSparkSession;
import utility.ProjectStaticVars;

import javax.inject.Inject;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CreateRandForestModelTest {

    private SparkSession spark;

    private String proloc;

    private JavaRDD<LabeledPoint> datasetRandomForest;

    @Inject
    ProjectStaticVars projectStaticVars;

    @Inject
    InstanceSparkSession instanceSparkSession;

    @Inject
    CreateRandForestModel createRandForestModel;

    @Inject
    PrepRawdata prepRawdata;

    @Before
    public void setUp() throws Exception {
        // test data
        spark = instanceSparkSession.getInstance();
        String fileIn = projectStaticVars.createFileLoc("/01_data/cstraining_kaggle.csv");
        ReadDataset dataset = new ReadDataset(spark, fileIn);
        dataset.readRawData();
        proloc = ProjectStaticVars.getProjloc();
        datasetRandomForest = prepRawdata.createRandForestData(
                dataset.getPreppedData()
        );

    }

    @Test
    public void createRandForestModel_shouldSucceed(){
        // method
        try {
            createRandForestModel.runModel(datasetRandomForest, spark, proloc);
        } catch (Exception e){
            fail("Exception not thrown");
        }
    }

    @Test(expected = IOException.class)
    @Ignore
    public void createRandForestModel_shouldThrowIOException_whenCannotCleanDir(){
    }

}
