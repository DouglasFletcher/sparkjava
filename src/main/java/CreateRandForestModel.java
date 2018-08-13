
import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import scala.Tuple2;

import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.SparkSession;


public class CreateRandForestModel {


    public static void createRandForestModel(JavaRDD<LabeledPoint> datasetIn, SparkSession spark, String proloc){
        // *************************
        // create training test data
        // *************************
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
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    private static final long serialVersionUID = 1L;
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<>(model.predict(p.features()), p.label());
                    }
                });

        Double testErr = 1.0 * predictionAndLabel.filter(
                new Function<Tuple2<Double, Double>, Boolean>() {
                    private static final long serialVersionUID = 1L;
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();
        System.out.println("Test Error Random Forest: " + testErr);
        //System.out.println("Learned classification forest model:\n" + model.toDebugString());

        // save model
        String output = proloc.concat("/02_model/RandomForest");
        File outputLoc = new File(output);
        try {
            FileUtils.deleteDirectory(outputLoc);
        } catch (IOException e) {
            e.printStackTrace();
        }
        model.save(spark.sparkContext(), output);

    }
}

