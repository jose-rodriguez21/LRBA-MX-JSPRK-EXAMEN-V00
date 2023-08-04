package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class JobExamenBuilderTest {

    private JobExamenBuilder jobExamenBuilder;

    @BeforeEach
    void setUp() {
        this.jobExamenBuilder = new JobExamenBuilder();
    }

    @Test
    void registerSources_na_SourceList() {
        final SourcesList sourcesList = this.jobExamenBuilder.registerSources();
        assertNotNull(sourcesList);
        assertNotNull(sourcesList.getSources());
        assertEquals(3, sourcesList.getSources().size());

        final Source source1 = sourcesList.getSources().get(0);
        assertNotNull(source1);
        assertEquals("sourceAlias1", source1.getAlias());
        assertEquals("characteristics.csv", source1.getPhysicalName());

        final Source source2 = sourcesList.getSources().get(1);
        assertNotNull(source2);
        assertEquals("sourceAlias2", source2.getAlias());
        assertEquals("movies.csv", source2.getPhysicalName());

        final Source source3 = sourcesList.getSources().get(2);
        assertNotNull(source3);
        assertEquals("sourceAlias3", source3.getAlias());
        assertEquals("streaming.csv", source3.getPhysicalName());
    }

    @Test
    void registerTransform_na_Transform() {
        //IF YOU WANT TRANSFORM CLASS
        final TransformConfig transformConfig = this.jobExamenBuilder.registerTransform();
        assertNotNull(transformConfig);
        assertNotNull(transformConfig.getTransform());
        //IF YOU WANT SQL TRANSFORM
        //final TransformConfig transformConfig = this.jobExamenBuilder.registerTransform();
        //assertNotNull(transformConfig);
        //assertNotNull(transformConfig.getTransformSqls());
        //IF YOU DO NOT WANT TRANSFORM
        //final TransformConfig transformConfig = this.jobExamenBuilder.registerTransform();
        //assertNull(transformConfig);
    }

    @Test
    void registerTargets_na_TargetList() {
        final TargetsList targetsList = this.jobExamenBuilder.registerTargets();
        assertNotNull(targetsList);
        assertNotNull(targetsList.getTargets());
        assertEquals(5, targetsList.getTargets().size());

        final Target target1 = targetsList.getTargets().get(0);
        assertNotNull(target1);
        assertEquals("targetAlias1", target1.getAlias());
        assertEquals("output/ejercicio1.csv", target1.getPhysicalName());

        final Target target2 = targetsList.getTargets().get(1);
        assertNotNull(target2);
        assertEquals("targetAlias2", target2.getAlias());
        assertEquals("output/ejercicio2.csv", target2.getPhysicalName());

        final Target target3 = targetsList.getTargets().get(2);
        assertNotNull(target3);
        assertEquals("targetAlias3", target3.getAlias());
        assertEquals("output/ejercicio3.csv", target3.getPhysicalName());

        final Target target4 = targetsList.getTargets().get(3);
        assertNotNull(target4);
        assertEquals("targetAlias4", target4.getAlias());
        assertEquals("output/ejercicio4.csv", target4.getPhysicalName());

        final Target target5 = targetsList.getTargets().get(4);
        assertNotNull(target5);
        assertEquals("targetAlias5", target5.getAlias());
        assertEquals("output/ejercicio5.csv", target5.getPhysicalName());
    }

}