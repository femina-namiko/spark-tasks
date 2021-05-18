package org.smagina.spark;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

public class Task2Test {

    @Test
    public void calculateHotelStatistic() {
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        createStructField("hotel_id", StringType, false),
                        createStructField("srch_ci", StringType, false),
                        createStructField("srch_co", StringType, false),
                });
        SparkSession spark = SparkSession.builder().config("spark.master", "local[*]").getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        List<Row> data = ImmutableList.of(
                RowFactory.create("1", "2017-01-01", "2017-01-01"),
                RowFactory.create("1", "2017-01-01", "2017-03-02"),
                RowFactory.create("1", "2017-01-01", "2017-01-02"),
                RowFactory.create("1", "2017-01-01", "2017-01-03"),
                RowFactory.create("1", "2017-01-01", "2017-01-09"),
                RowFactory.create("1", "2017-01-01", "2017-01-22")

        );
        Dataset<Row> ds = spark.createDataFrame(data, schema);
        ds.show();
        Dataset<Row> ds_result = Task2.calculateHotelStatistic(ds);
        ds_result.show();

        List<Row> result = ds_result.collectAsList();

        Assert.assertEquals(1, result.size());
        Row r0 = result.get(0);
        Assert.assertEquals(2, r0.getLong(r0.fieldIndex("erroneous_data_cnt")));
        Assert.assertEquals(1, r0.getLong(r0.fieldIndex("short_stay_cnt")));
        Assert.assertEquals(1, r0.getLong(r0.fieldIndex("standart_stay_cnt")));
        Assert.assertEquals(1, r0.getLong(r0.fieldIndex("standart_extended_stay_cnt")));
        Assert.assertEquals(1, r0.getLong(r0.fieldIndex("long_stay_cnt")));
    }
}