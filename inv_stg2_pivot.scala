import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File

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
    val dataFiles = getListOfFiles("..\\esbi_stream\\app\\data")

    val spark = SparkSession.builder.appName("Inv_Stg2").getOrCreate()

    val stg1DF = spark
                  .read
                  .format("csv")
                  .options(Map("header" -> "true", "inferschema" -> "true"))
                  .load("..\\esbi_stream\\app\\data\\*.csv")
    
    stg1DF.columns.mkString(", ")

    // stg1DF.show()
    // Start data pivot:
    
    // Sum: Inverter level must sum values
    // Seems to work for all other tags as well as long as one record and no duplicates exist:
    // fIND A WAY TO COUNT THE NUMBER OF TAGNAMES PER TAG TYPE: IF COUNT() > 1 THEN SUM() ELSE MAX()
    val sumStg2DF = stg1DF
      .groupBy("PLANT_ID", "TIMESTAMPLOCAL")
      .pivot("TAG_TYPE")
      .agg(sum("VALUE"))

    sumStg2DF.show()

    // Want to use in case duplciates exist:
    val maxStg2DF = stg1DF
      .groupBy("PLANT_ID", "TIMESTAMPLOCAL")
      .pivot("TAG_TYPE")
      .agg(max("VALUE"))

    maxStg2DF.show()

    spark.close()

  }
}
