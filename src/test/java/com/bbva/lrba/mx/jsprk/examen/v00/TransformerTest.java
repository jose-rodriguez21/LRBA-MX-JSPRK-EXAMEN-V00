package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import com.bbva.lrba.mx.jsprk.examen.v00.model.RowDataEjercicio1;
import com.bbva.lrba.mx.jsprk.examen.v00.model.RowDataEjercicio2;
import com.bbva.lrba.mx.jsprk.examen.v00.model.RowDataEjercicio3;
import com.bbva.lrba.mx.jsprk.examen.v00.model.RowDataEjercicio4;
import com.bbva.lrba.mx.jsprk.examen.v00.model.RowDataEjercicio5;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema1 = DataTypes.createStructType(
               new StructField[]{
                       DataTypes.createStructField("title", DataTypes.StringType, false),
                       DataTypes.createStructField("country", DataTypes.StringType, false),
                       DataTypes.createStructField("dateAdded", DataTypes.StringType, false),
                       DataTypes.createStructField("releaseYear", DataTypes.StringType, false),
                       DataTypes.createStructField("rating", DataTypes.StringType, false),
                       DataTypes.createStructField("duration", DataTypes.StringType, false),
                       DataTypes.createStructField("listedIn", DataTypes.StringType, false),
               });
        Row firstRow1 = RowFactory.create("Dick Johnson Is Dead","United States","September 25, 2021","2020","PG-13","182 min","Documentaries");
        Row secondRow1 = RowFactory.create("Blood & Water","South Africa","September 24, 2021","2021","TV-MA","2 Seasons","International TV Shows, TV Dramas, TV Mysteries");
        Row thirdRow1 = RowFactory.create("Ganglands","","September 24, 2021","2021","TV-MA","1 Season","Crime TV Shows, International TV Shows, TV Action & Adventure");
        Row fourRow1 = RowFactory.create("Jailbirds New Orleans","","September 24, 2021","2021","TV-MA","1 Season","Docuseries, Reality TV");
        Row fiveRow1 = RowFactory.create("Soy Luna","Argentina, Mexico","September 18, 2020","2015","TV-G","3 Seasons","Comedy, Coming of Age, Drama");

        final List<Row> listRows1 = Arrays.asList(firstRow1, secondRow1, thirdRow1, fourRow1, fiveRow1);

        StructType schema2 = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("title", DataTypes.StringType, false),
                        DataTypes.createStructField("director", DataTypes.StringType, false),
                        DataTypes.createStructField("cast", DataTypes.StringType, false),
                        DataTypes.createStructField("description", DataTypes.StringType, false),
                        DataTypes.createStructField("code", DataTypes.StringType, false),
                        DataTypes.createStructField("type", DataTypes.StringType, false),
                });
        Row firstRow2 = RowFactory.create("Dick Johnson Is Dead","Kirsten Johnson","","As her father nears the end of his life, filmmaker Kirsten Johnson stages his death in inventive and comical ways to help them both face the inevitable.","NFLX","Movie");
        Row secondRow2 = RowFactory.create("Blood & Water","","Ama Qamata, Khosi Ngema, Gail Mabalane, Thabang Molaba, Dillon Windvogel, Natasha Thahane, Arno Greeff, Xolile Tshabalala, Getmore Sithole, Cindy Mahlangu, Ryle De Morny, Greteli Fincham, Sello Maake Ka-Ncube, Odwa Gwanya, Mekaila Mathys, Sandi Schultz, Duane Williams, Shamilla Miller, Patrick Mofokeng","After crossing paths at a party, a Cape Town teen sets out to prove whether a private-school swimming star is her sister who was abducted at birth.","NFLX","TV Show");
        Row thirdRow2 = RowFactory.create("Ganglands","Julien Leclercq","Sami Bouajila, Tracy Gotoas, Samuel Jouy, Nabiha Akkari, Sofia Lesaffre, Salim Kechiouche, Noureddine Farihi, Geert Van Rampelberg, Bakary Diombera","To protect his family from a powerful drug lord, skilled thief Mehdi and his expert team of robbers are pulled into a violent and deadly turf war.","NFLX","TV Show");
        Row fourRow2 = RowFactory.create("Star Wars Vintage: Story of the Faithful Wookiee","","","With his friends ailing from a sleeping virus, Chewbacca gets “help” from Boba Fett.","DSNY+","MOVIE");
        Row fiveRow2 = RowFactory.create("Journey Through The Stars","Mark Knight","","An out-of-this-world experience where we dance among the stars of our vast solar system. The 432 hz binaural beat sound bed enhances the parasympathetic nervous system-the source of relaxation, and slows down the sympathetic nervous system-the fight-or-flight side of the nervous system. The outcomes are less stress and inflammation in your body, ensuring a happier, healthier You.","AMZN","Movie");

        final List<Row> listRows2 = Arrays.asList(firstRow2, secondRow2, thirdRow2, fourRow2, fiveRow2);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> characteristics = datasetUtils.createDataFrame(listRows1, schema1);
        Dataset<Row> movies = datasetUtils.createDataFrame(listRows2, schema2);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("sourceAlias1", characteristics, "sourceAlias2", movies)));
        assertNotNull(datasetMap);
        assertEquals(5, datasetMap.size());

        /* Test Ejercicio 1 */
        Dataset<RowDataEjercicio1> returnedDs1 = datasetMap.get("targetAlias1").as(Encoders.bean(RowDataEjercicio1.class));
        returnedDs1.show();
        final List<RowDataEjercicio1> rows1 = datasetToTargetData(returnedDs1, RowDataEjercicio1.class);

        assertEquals(10, rows1.size());
        assertEquals("Dick Johnson Is Dead", rows1.get(0).getTitle());
        assertEquals(null, rows1.get(0).getCountry());
        assertEquals("Dick Johnson Is Dead", rows1.get(5).getTitle());
        assertEquals("United States", rows1.get(5).getCountry());

        /* Test Ejercicio 2 */
        Dataset<RowDataEjercicio2> returnedDs2 = datasetMap.get("targetAlias2").as(Encoders.bean(RowDataEjercicio2.class));
        returnedDs2.show();
        final List<RowDataEjercicio2> rows2 = datasetToTargetData(returnedDs2, RowDataEjercicio2.class);

        assertEquals(2, rows2.size());
        assertEquals("Netflix", rows2.get(0).getService());
        assertEquals(BigInteger.ONE, rows2.get(0).getCount());

        /* Test Ejercicio 3 */
        Dataset<RowDataEjercicio3> returnedDs3 = datasetMap.get("targetAlias3").as(Encoders.bean(RowDataEjercicio3.class));
        returnedDs3.show();
        final List<RowDataEjercicio3> rows3 = datasetToTargetData(returnedDs3, RowDataEjercicio3.class);

        assertEquals(1, rows3.size());
        assertEquals("Mark Knight", rows3.get(0).getDirector());
        assertEquals("AMZN",rows3.get(0).getCode());
        assertEquals("Amazon Prime",rows3.get(0).getService());

        /* Test Ejercicio 4 */
        Dataset<RowDataEjercicio4> returnedDs4 = datasetMap.get("targetAlias4").as(Encoders.bean(RowDataEjercicio4.class));
        returnedDs4.show();
        final List<RowDataEjercicio4> rows4 = datasetToTargetData(returnedDs4, RowDataEjercicio4.class);

        assertEquals(1, rows4.size());
        assertEquals("182 min", rows4.get(0).getDuration());

        /* Test Ejercicio 5 */
        Dataset<RowDataEjercicio5> returnedDs5 = datasetMap.get("targetAlias5").as(Encoders.bean(RowDataEjercicio5.class));
        returnedDs5.show();
        final List<RowDataEjercicio5> rows5 = datasetToTargetData(returnedDs5, RowDataEjercicio5.class);

        assertEquals(2, rows5.size());
        assertEquals("Matt Groening", rows5.get(0).getDirector());
        assertEquals("George Lucas", rows5.get(1).getDirector());
    }

}