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

    // val stg2DF = spark.read.format("csv").option("header", "false").load("..\\esbi_stream\\app\\data\\*.csv")
  
    val stg1DF = spark
                  .read
                  .format("csv")
                  .options(Map("header" -> "true", "inferschema" -> "true"))
                  .load("..\\esbi_stream\\app\\data\\*.csv")
    
    stg1DF.columns.mkString(", ")

    // stg1DF.show()
    // Start data pivot:
    val stg2DF = stg1DF
      .groupBy("PLANT_ID", "TIMESTAMPLOCAL")
      .pivot("TAG_NAME")
      .agg(sum("VALUE"))

    stg2DF.show()

  }
}
