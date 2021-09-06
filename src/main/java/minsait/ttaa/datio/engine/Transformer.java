package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {
    private SparkSession spark;

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = exampleWindowFunction(df);
        df = defineRule2(df);
        df = defineRule3(df);
        df = defineRule4(df);
        df = filterConditions(df);
        df = columnSelection(df);




        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);

        df.printSchema();

        // Uncomment when you want write your final output
        //write(df);
    }

    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                catHeightByPosition.column(),
                ageRange.column(),
                rankByNationalityPosition.column(),
                potentialVsOverall.column()


        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }

    private Dataset<Row> defineRule2(Dataset<Row> df) {
        Column rule = when(df.col("age").$less(23), "A")
                .when(df.col("age").$less(27), "B")
                .when(df.col("age").$less(32), "C")
                .otherwise("D");

        df = df.withColumn(ageRange.getName(), rule);

        return df;
    }

    private Dataset<Row> defineRule3(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());

        Column rank = row_number().over(w);

        df = df.withColumn(rankByNationalityPosition.getName(), rank);

        return df;
    }


    private Dataset<Row> defineRule4(Dataset<Row> df) {
        Column rule = df.col("potential").$div(df.col("overall"));
        df = df.withColumn(potentialVsOverall.getName(), rule);
        return df;
    }

    private Dataset<Row> filterConditions(Dataset<Row> df) {
        df = df.filter(
                        rankByNationalityPosition.column().$less(3)
                        .or(ageRange.column().equalTo("B")
                                .or(ageRange.column().equalTo("C"))
                                .and(potentialVsOverall.column().$greater(1.15)))
                        .or(ageRange.column().equalTo("A")
                                .and(potentialVsOverall.column().$greater(1.25)))
                        .or(ageRange.column().equalTo("D")
                                .and(rankByNationalityPosition.column().$less(5)))
        );
        return df;
    }
/*
    private Dataset<Row> filterCondition1(Dataset<Row> df) {
        df = df.filter(
                rankByNationalityPosition.column().$less(3));
        return df;
    }

    private Dataset<Row> filterCondition2(Dataset<Row> df) {
        df = df.filter(ageRange.column().equalTo("B")
                .or(ageRange.column().equalTo("C"))
                .and(potentialVsOverall.column().$greater(1.15)));
        return df;
    }

    private Dataset<Row> filterCondition3(Dataset<Row> df) {
        df = df.filter(ageRange.column().equalTo("A")
                .and(potentialVsOverall.column().$greater(1.25)));
        return df;
    }

    private Dataset<Row> filterCondition4(Dataset<Row> df) {
        df = df.filter(ageRange.column().equalTo("D")
                .and(rankByNationalityPosition.column().$less(5)));
        return df;
    }
*/

}
