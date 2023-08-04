package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.builder.annotation.Builder;
import com.bbva.lrba.builder.spark.RegisterSparkBuilder;
import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;

@Builder
public class JobExamenBuilder extends RegisterSparkBuilder {

    final static public String LOGICALDATASTORE = "local.logicalDataStore.batch";

    @Override
    public SourcesList registerSources() {
        //EXAMPLE WITH A LOCAL SOURCE FILE
        return SourcesList.builder()
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias1")
                        .physicalName("characteristics.csv")
                        .serviceName(LOGICALDATASTORE)
                        .sql("SELECT * FROM sourceAlias1")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias2")
                        .physicalName("movies.csv")
                        .serviceName(LOGICALDATASTORE)
                        .sql("SELECT * FROM sourceAlias2")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("sourceAlias3")
                        .physicalName("streaming.csv")
                        .serviceName(LOGICALDATASTORE)
                        .sql("SELECT * FROM sourceAlias3")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

    @Override
    public TransformConfig registerTransform() {
        //IF YOU WANT TRANSFORM CLASS
        return TransformConfig.TransformClass.builder().transform(new Transformer()).build();
        //IF YOU WANT SQL TRANSFORM
        //return TransformConfig.SqlStatements.builder().addSql("targetAlias1", "SELECT CAMPO1 FROM sourceAlias1").build();
        //IF YOU DO NOT WANT TRANSFORM
        //return null;
    }

    @Override
    public TargetsList registerTargets() {
        //EXAMPLE WITH A LOCAL TARGET FILE
        return TargetsList.builder()
                .add(Target.File.Csv.builder()
                        .alias("targetAlias1")
                        .physicalName("output/ejercicio1.csv")
                        .serviceName(LOGICALDATASTORE)
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Target.File.Csv.builder()
                        .alias("targetAlias2")
                        .physicalName("output/ejercicio2.csv")
                        .serviceName(LOGICALDATASTORE)
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Target.File.Csv.builder()
                        .alias("targetAlias3")
                        .physicalName("output/ejercicio3.csv")
                        .serviceName(LOGICALDATASTORE)
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Target.File.Csv.builder()
                        .alias("targetAlias4")
                        .physicalName("output/ejercicio4.csv")
                        .serviceName(LOGICALDATASTORE)
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Target.File.Csv.builder()
                        .alias("targetAlias5")
                        .physicalName("output/ejercicio5.csv")
                        .serviceName(LOGICALDATASTORE)
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

}