import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import java.io.File
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

	// Sum: Inverter level must sum values
	// Seems to work for all other tags as well as long as one record and no duplicates exist:
	// fIND A WAY TO COUNT THE NUMBER OF TAGNAMES PER TAG TYPE: IF COUNT() > 1 THEN SUM() ELSE MAX()

	val stg2_df = stg1DF
	  .groupBy("PLANT_ID", "TIMESTAMPLOCAL")
	  .pivot("TAG_TYPE")
	  .agg(
		when(col("TAG_TYPE").rlike("INDIV_"),sum("VALUE")).otherwise(max("VALUE"))
	  )

        // Add new columns to DF with new logic:
        val stg2_calc_df = stg2_df.withColumn("DOWN_TIME_AREG",
                            expr("case when (PJM_AREG_DOWN + PJM_AREG_DOWN) / 2 < 10 THEN (1/60)*(10-((PJM_AREG_DOWN + PJM_AREG_DOWN)/2)/10) ELSE 0 END"))

        // Register DatFrame as a temp table:
        stg2_calc_df.createOrReplaceTempView("es_inv_stg2")
        stg2_calc_df.cache()

        val stg2AggDF = spark.sql("""
                                    SELECT DISTINCT plant_id, from_unixtime(unix_timestamp(TIMESTAMPLOCAL, 'dd-MMM-yyTHH:mm:ss')) DAY_DATE, SUM(SITE_POWER) 
                                    FROM es_inv_stg2 
                                    GROUP BY plant_id, from_unixtime(unix_timestamp(TIMESTAMPLOCAL, 'dd-MMM-yyTHH:mm:ss'))
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
