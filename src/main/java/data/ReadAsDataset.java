package data;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class ReadAsDataset {

    private SparkSession spark;
    private String fileIn;
    private Dataset<Row> rawData;

    public ReadAsDataset(SparkSession spark, String fileIn){
        this.spark = spark;
        this.fileIn = fileIn;
        this.getPreppedData();
    }

    private void readRawData() {

        // define schema
        StructType schema = new StructType()
                .add("SeriousDlqin2yrs", "string")
                .add("SeriousDlqin2yrs", "string")
                .add("RevolvingUtilizationOfUnsecuredLines", "string")
                .add("age", "string")
                .add("NumberOfTime3059DaysPastDueNotWorse", "string")
                .add("DebtRatio", "string")
                .add("MonthlyIncome", "string")
                .add("NumberOfOpenCreditLinesAndLoans", "string")
                .add("NumberOfTimes90DaysLate", "string")
                .add("NumberRealEstateLoansOrLines", "string")
                .add("NumberOfTime6089DaysPastDueNotWorse", "string")
                .add("NumberOfDependents", "string");

        // read data
        rawData = spark.read()
                .option("mode", "DROPMALFORMED")
                .schema(schema)
                .csv(fileIn);
    }

    public Dataset<Row> getPreppedData() {
        return rawData;
    }

}
