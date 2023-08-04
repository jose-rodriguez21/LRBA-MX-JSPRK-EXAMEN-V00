package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.util.HashMap;
import java.util.Map;

import static com.bbva.lrba.mx.jsprk.examen.v00.utils.Constants.*;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        Dataset<Row> characteristicsDS = datasetsFromRead.get("sourceAlias1");
        Dataset<Row> moviesDS = datasetsFromRead.get("sourceAlias2");

        Dataset<Row> moviesWithStreaming = moviesDS.select(moviesDS.col(ALL_COLUMNS.getValue()),
                functions.when(moviesDS.col(CODE_COLUMN.getValue()).equalTo(CODE_NETFLIX.getValue()), SERVICE_NETFLIX.getValue())
                .when(moviesDS.col(CODE_COLUMN.getValue()).equalTo(CODE_DISNEY.getValue()), SERVICE_DISNEY.getValue())
                .when(moviesDS.col(CODE_COLUMN.getValue()).equalTo(CODE_AMAZON.getValue()), SERVICE_AMAZON.getValue()).alias(SERVICE_COLUMN.getValue()));

        /* Test Ejercicio 1 */
        Dataset<Row> unionAllTables = moviesWithStreaming.unionByName(characteristicsDS, true);
        unionAllTables = unionAllTables.select(unionAllTables.col(ALL_COLUMNS.getValue()), functions.lit(functions.current_timestamp()).alias("date"));

        /* Test Ejercicio 2 */
        Dataset<Row> countMoviesForService = moviesWithStreaming.filter(functions.col(TYPE_COLUMN.getValue()).equalTo(MOVIE_TYPE.getValue()))
                .groupBy(SERVICE_COLUMN.getValue()).count().as(NO_MOVIES_COLUMN.getValue());
        countMoviesForService = countMoviesForService.select(countMoviesForService.col(ALL_COLUMNS.getValue()), functions.lit(functions.current_timestamp()).alias("date"));

        /* Test Ejercicio 3 */
        Dataset<Row> directoresForStreaming = moviesWithStreaming.filter(functions.col(SERVICE_COLUMN.getValue()).equalTo(SERVICE_AMAZON.getValue()))
                .filter(functions.col(TYPE_COLUMN.getValue()).equalTo(MOVIE_TYPE.getValue()))
                .orderBy(functions.col(DIRECTOR_COLUMN.getValue()));
        directoresForStreaming = directoresForStreaming.select(directoresForStreaming.col(ALL_COLUMNS.getValue()), functions.lit(functions.current_timestamp()).alias("date"));

        /* Test Ejercicio 4 */
        Dataset<Row> seriesMoviesLarge = characteristicsDS.filter(functions.col(DURATION_COLUMN.getValue()).equalTo("10 Seasons")
                .or(functions.col(DURATION_COLUMN.getValue()).equalTo("182 min")));
        seriesMoviesLarge = seriesMoviesLarge.select(seriesMoviesLarge.col(ALL_COLUMNS.getValue()), functions.lit(functions.current_timestamp()).alias("date"));

        /* Test Ejercicio 5 */
        Dataset<Row> ejercicio5 = moviesDS.select(moviesDS.col(ALL_COLUMNS.getValue()),
                functions.when(moviesDS.col(DIRECTOR_COLUMN.getValue()).rlike("[A-b]"), "SI")
                        .otherwise("NO").alias("aux"));

        ejercicio5 = ejercicio5.filter(ejercicio5.col("aux").equalTo("NO")).drop("aux")
                .select(ejercicio5.col(TITLE_COLUMN.getValue()),
                        functions.when(functions.col(TYPE_COLUMN.getValue()).equalTo("TV Show"), "Matt Groening")
                        .when(functions.col(TYPE_COLUMN.getValue()).equalTo("MOVIE"), "George Lucas").alias(DIRECTOR_COLUMN.getValue()),
                        ejercicio5.col(CAST_COLUMN.getValue()),
                        ejercicio5.col(DESCRIPTION_COLUMN.getValue()),
                        ejercicio5.col(CODE_COLUMN.getValue()),
                        ejercicio5.col(TYPE_COLUMN.getValue()));

        ejercicio5 = ejercicio5.select(ejercicio5.col(ALL_COLUMNS.getValue()), functions.lit(functions.current_timestamp()).alias("date"));

        datasetsToWrite.put("targetAlias1", unionAllTables);
        datasetsToWrite.put("targetAlias2", countMoviesForService);
        datasetsToWrite.put("targetAlias3", directoresForStreaming);
        datasetsToWrite.put("targetAlias4", seriesMoviesLarge);
        datasetsToWrite.put("targetAlias5", ejercicio5);

        return datasetsToWrite;
    }

}