package modelling;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import base.config.SJConfig;
import scala.Tuple2;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.SparkSession;

/**
 * <h3>Create Model</h3>
 * <p>Model to create random forest model</p>
 */
public class CreateRandForestModel {

    /**
     * <h3>run model</h3>
     * <p>run random forest model</p>
     * @param datasetIn JavaRDD<LabeledPoint>: modelling data
     * @param spark SparkSession: spark session
     * @param proloc String: project location
     */
    public static void runModel(JavaRDD<LabeledPoint> datasetIn, SparkSession spark, String proloc){

        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = datasetIn.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // Train a RandomForest model.
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 200;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        Integer maxDepth = 10;
        Integer maxBins = 32;
        Integer seed = 12345;
        final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);



        // Evaluate model on test instances and compute test error
        Double testErr = testData.map(s -> {//
            return new Tuple2<Double, Double>(model.predict(s.features()), s.label());//
        }).filter(//
            s -> s._1.equals(s._2)//
        ).count() / testData.count() * 1.0;//
        System.out.println("Test Error Random Forest: " + testErr);

        // save model
        String output = proloc.concat(SJConfig.projectLocOutdirConfig.getValue());
        File outputLoc = new File(output);
        try {
            FileUtils.deleteDirectory(outputLoc);
        } catch (IOException e) {
            e.printStackTrace();
        }
        model.save(spark.sparkContext(), output);
    }

}

