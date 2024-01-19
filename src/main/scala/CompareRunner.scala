import SparkUtilities.{compareDFs, flattenDataFrame}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object CompareRunner {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Test Compare")
    .getOrCreate()

  val USER_HOME_DIR = System.getProperty("user.home")

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger("")
    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)
    val rootPath = s"$USER_HOME_DIR/Repos/my-spark-utilities/src/main/resources"
    val fileName1 = "sample.json"
    val fileName2 = "sample.json"
    val df1 = spark.read.option("multiline", "true").json(s"$rootPath/$fileName1")
    val df2 = spark.read.option("multiline", "true").json(s"$rootPath/$fileName2")

    val result = compareDFs(flattenDataFrame(df1, true), flattenDataFrame(df2, true))
    logger.info(s"dataframe1 is equal to dataframe2: $result")
  }
}
