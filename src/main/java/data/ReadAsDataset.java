package data;

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
        this.readRawData();
    }

    private void readRawData() {

        // define schema
        StructType schema = new StructType()
                .add("SeriousDlqin2yrs", "string") // 1
                .add("RevolvingUtilizationOfUnsecuredLines", "string") // 2
                .add("age", "string") // 3
                .add("NumberOfTime3059DaysPastDueNotWorse", "string") // 4
                .add("DebtRatio", "string") // 5
                .add("MonthlyIncome", "string") // 6
                .add("NumberOfOpenCreditLinesAndLoans", "string") // 7
                .add("NumberOfTimes90DaysLate", "string") // 8
                .add("NumberRealEstateLoansOrLines", "string") // 9
                .add("NumberOfTime6089DaysPastDueNotWorse", "string") // 10
                .add("NumberOfDependents", "string"); // 11

        // read data
        rawData = spark.read()
                .option("header","true")
                .schema(schema)
                .csv(fileIn);
    }

    public Dataset<Row> getPreppedData() {
        return rawData;
    }

}
