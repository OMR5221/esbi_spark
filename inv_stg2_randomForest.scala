import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
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

      val spark = SparkSession.builder.appName("Inv_Stg2").getOrCreate()

      val stg1DF = spark
                    .read
                    .format("csv")
                    .options(Map("header" -> "true", "inferschema" -> "true"))
                    .load("..\\esbi_stream\\data\\*.csv")

      stg1DF.columns.mkString(", ")

      case class TagTypeConfigs(plantID: Int, patterns: Map[String, String])

      val plantPatterns = Map[Int, Map[String, String]]()

      plantPatterns += (
                          34044 -> Map( "INV_ACTIVE_POWER" -> "(INV)([0-9]{3}[0-9])",
                                        "INV_AVAIL" -> "(INV)([0-9]{3}[0-9])",
                                        "RACKS_AVAIL_BANK" -> "(BSC|BANK)([0-9]{2}[0-9])"
                                      )
                       )

      val levelInstPattern = """"(INV)([0-9]{3}[0-9])"""

      // Add part from tagname value column:
      val stg1_lvlDF = stg1DF.withColumn("LEVEL", regexp_extract(col("TAG_NAME"), plantPatterns(34044)("INV_ACTIVE_POWER"), 1))
      val stg1_instDF = stg1_lvlDF.withColumn("INSTANCE", regexp_extract(col("TAG_NAME"), plantPatterns(34044)("INV_ACTIVE_POWER"), 2))

      val stg1_filt_df = stg1_instDF.filter(col("TAG_NAME") === "BMY_xxxxxxx_INV0000_Site_Avail_Count")

      stg1_filt_df.show(50)

      // Sum: Inverter level must sum values
      // Seems to work for all other tags as well as long as one record and no duplicates exist:
      // fIND A WAY TO COUNT THE NUMBER OF TAGNAMES PER TAG TYPE: IF COUNT() > 1 THEN SUM() ELSE MAX()

      // need to parse tagname from stg1 if at inverter or rack level to get the inverter number:
      // Example: TAG TYPE: INDIV_INV_ACTIVE_POWER_
      // TAG NAME: BPB_BATTxxx_INV0001_KW
      // UPDATE TAG_TYPE: INDIV_INV_ACTIVE_POWER_0001
      // TAGNAME: _INV/d+_, _BANK/d+_, _CNV/d+_, _BAT/d+_

      val stg2_df = stg1DF
        .groupBy("PLANT_ID", "TIMESTAMPLOCAL")
        .pivot("TAG_TYPE")
        .agg(
          when(col("TAG_TYPE").rlike("INDIV_"), sum("VALUE")).otherwise(max("VALUE"))
        )


      

      // Add new columns to DF with new logic:
      val stg2_calc_df = stg2_df.withColumn("DOWN_TIME_AREG", expr("case when (PJM_AREG_DOWN + PJM_AREG_DOWN) / 2 < 10 THEN (1/60)*(10-((PJM_AREG_DOWN + PJM_AREG_DOWN)/2)/10) ELSE 0 END"))


        // Register DatFrame as a temp table:
        stg2_calc_df.createOrReplaceTempView("es_inv_stg2")
        stg2_calc_df.cache()

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
