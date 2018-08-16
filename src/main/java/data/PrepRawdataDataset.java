package data;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Dataset$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class PrepRawdataDataset {

    private Dataset<Row> datasetOut;

    /**
     * <h3>constructor</h3>
     * <p>run transformations</p>
     * @param datasetIn
     */
    public PrepRawdataDataset(Dataset<Row> datasetIn){
        this.createRandForestData(datasetIn);
    }

    /**
     * <h3>create modelling data</h3>
     * <p>adds transformations to modelling data</p>
     * @param datasetIn Dataset<Row>: input data
     */
    public void createRandForestData(Dataset<Row> datasetIn) {
        datasetOut = datasetIn
            .withColumn("new_col1", functions.lit(1))
            .withColumn("new_col2", functions.lit(2));
    }

    /**
     * <h3>get datasetout</h3>
     * @return datasetOut Dataset<Row>: data with transformations
     */
    public Dataset<Row> getDatasetOut(){
        return datasetOut;
    }

}
