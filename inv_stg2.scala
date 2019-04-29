import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.regexp_extract
import java.io.File
import collection.mutable.Map
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler}

object stg2 {


  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)

    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    }
    else {
      List[File]()
    }
  }

  def main(args: Array[String])
  {
      // Multithread per site-tag
      val dataFiles = getListOfFiles("..\\esbi_stream\\data")

      val spark = SparkSession.builder.appName("Inv_Stg2").config("spark.master", "local").getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")

      import spark.implicits._

      val stg1RawDF = spark
                    .read
                    .format("csv")
                    .options(Map("header" -> "true", "inferschema" -> "true"))
                    .load("..\\esbi_stream\\data\\1230*.csv")

      stg1RawDF.columns.mkString(", ")

      stg1RawDF.head.schema.printTreeString() 

      case class ScadaSTG1( PLANT_ID: Int, PLANT_CODE: String, TAGNAME: String, VALUE: Double, TIMESTAMPLOCAL: String, TIMESTAMPUTC: String, CREATE_DATE: String, UPDATE_DATE: String,
                            CREATED_BY: String, UPDATED_BY: String
      )

      case class TagTypeConfigs(plantID: Int, patterns: Map[String, String])

      // Map plant ID the expected inverter level tag types
      val plantTTMaps = Map[Int, Map[String, String]]()

      // Map tag type to pattern:
      plantTTMaps += (
                          34044 -> Map( "INDIV_INV_ACTIVE_POWER" -> "(INV)([0-9]{3}[0-9])",
                                        "INDIV_INV_AVAIL" -> "(INV)([0-9]{3}[0-9])",
                                        "INDIV_RACKS_AVAIL_BANK" -> "(BSC|BANK)([0-9]{2}[0-9])"
                                  ),
                          1230 -> Map( "INDIV_INV_ACTIVE_POWER" -> "(INV)([0-9]{3}[0-9])",
                                       "INDIV_INV_AVAIL" -> "(INV)([0-9]{3}[0-9])",
                                       "INDIV_RACKS_AVAIL_BANK" -> "(BSC|BANK)([0-9]{2}[0-9])"
                                  ),
                          1231 -> Map( "INDIV_INV_ACTIVE_POWER" -> "(INV)([0-9]{3}[0-9])",
                                       "INDIV_INV_AVAIL" -> "(INV)([0-9]{3}[0-9])",
                                       "INDIV_RACKS_AVAIL_BANK" -> "(BSC|BANK)([0-9]{2}[0-9])"
                                  )

                       )

      // Create a dataframe from the map?
      val plantTTDS = plantTTMaps.toSeq.toDF("PLANT_ID", "TAG_TYPE_REGEXP")

      plantTTDS.show(50)

      val levelInstPattern = """"(INV)([0-9]{3}[0-9])"""

      // spark.udf.register("getIntUDF", (v: Int) => v * v)

      // Add part from tagname value column:
      val stg1LvlDF = stg1RawDF.withColumn("LEVEL", regexp_extract(col("TAG_NAME"), plantTTMaps.get(1230).get("INDIV_INV_ACTIVE_POWER"), 1))
      val stg1InstDF = stg1LvlDF.withColumn("INSTANCE", regexp_extract(col("TAG_NAME"), plantTTMaps.get(1230).get("INDIV_INV_ACTIVE_POWER"), 2))

      val stg1FiltDF= stg1InstDF.filter(col("TAG_NAME") === "BEM_BATTxxx_INV0001_kW")

      val stg1FinalDF = stg1FiltDF.withColumn("UNIQUE_TAG_TYPE", concat(col("TAG_TYPE"), lit("_"), col("LEVEL"), lit("_"), col("INSTANCE")))
      stg1FinalDF.show(50)

      // Sum: Inverter level must sum values
      // Seems to work for all other tags as well as long as one record and no duplicates exist:
      // fIND A WAY TO COUNT THE NUMBER OF TAGNAMES PER TAG TYPE: IF COUNT() > 1 THEN SUM() ELSE MAX()

      // need to parse tagname from stg1 if at inverter or rack level to get the inverter number:
      // Example: TAG TYPE: INDIV_INV_ACTIVE_POWER_
      // TAG NAME: BPB_BATTxxx_INV0001_KW
      // UPDATE TAG_TYPE: INDIV_INV_ACTIVE_POWER_0001
      // TAGNAME: _INV/d+_, _BANK/d+_, _CNV/d+_, _BAT/d+_

      /*
      val stg2PivotDF = stg1FinalDF
        .groupBy("PLANT_ID", "TIMESTAMPLOCAL")
        .pivot("TAG_TYPE")
        .agg(
          when(col("TAG_TYPE").rlike("INDIV_"), sum("VALUE")).otherwise(max("VALUE"))
        )
      */

      val stg2PivotDF = stg1FinalDF
        .groupBy("PLANT_ID", "TIMESTAMPLOCAL")
        .pivot("UNIQUE_TAG_TYPE")
        .agg(
          when(col("UNIQUE_TAG_TYPE").rlike("INDIV_"), sum("VALUE")).otherwise(max("VALUE"))
        )

      stg2PivotDF.show(100)

      /* Add new columns to DF with new logic:
      val stg2CalcDF = stg2PivotDF.withColumn("DOWN_TIME_AREG", expr("case when (PJM_AREG_DOWN + PJM_AREG_DOWN) / 2 < 10 THEN (1/60)*(10-((PJM_AREG_DOWN + PJM_AREG_DOWN)/2)/10) ELSE 0 END"))

      // Register DatFrame as a temp table:
      stg2CalcDF.createOrReplaceTempView("es_inv_stg2")
      stg2CalcDF.cache()

      val stg2AggDF = spark.sql("""
                                SELECT DISTINCT
                                  plant_id,
                                  from_unixtime(unix_timestamp(TIMESTAMPLOCAL, 'dd-MMM-yyTHH:mm:ss')) DAY_DATE,
                                  SUM(SITE_POWER)
                                FROM es_inv_stg2
                                GROUP BY
                                  plant_id,
                                  from_unixtime(unix_timestamp(TIMESTAMPLOCAL, 'dd-MMM-yyTHH:mm:ss'))
                                """
                        )

      stg2AggDF.show(50)
      */

      // Can now run sql statements against the registered Temp Table:

      // Load and parse the data file, converting it to a DataFrame.
      // val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

      // Prepare data for classification:
      // Create a feature column of all predictor values:
      /*
      val assembler = new VectorAssembler().setInputCols(Array( "SITE_POWER", "AUX_POWER", 
                                                                "PJM_AREG_DOWN", "INV_AVAILABLE_COUNT", 
                                                                "PJM_REGD")).setOutputCol("features")

      val stg2_ft_df = assembler.transform(stg2_df)

      stg2_ft_df.show()
      */

      spark.close()
  }
}
