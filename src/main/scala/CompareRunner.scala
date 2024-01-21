import SparkUtilities.{compareDFsSymDiff, flattenDataFrame}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object CompareRunner {

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Test Compare")
    .getOrCreate()

  val USER_HOME_DIR = System.getProperty("user.home")

  def main(args: Array[String]): Unit = {
    spark.sparkContext.setLogLevel("WARN")
    val logger = Logger.getLogger("CompareRunnerLogger")
    val rootPath = s"$USER_HOME_DIR/Repos/my-spark-utilities/src/main/resources"
    val fileName1 = "sample.json"
    val fileName2 = "sample2.json"
    val df1 = spark.read.option("multiline", "true").json(s"$rootPath/$fileName1")
    val df2 = spark.read.option("multiline", "true").json(s"$rootPath/$fileName2")

    val resultFalse = compareDFsSymDiff(flattenDataFrame(df1, true), flattenDataFrame(df2, true))
    logger.warn(s"dataframe1 ($fileName1) is equal to dataframe2 ($fileName2): $resultFalse")

    val resultTrue = compareDFsSymDiff(flattenDataFrame(df1, true), flattenDataFrame(df1, true))
    logger.warn(s"dataframe1 ($fileName1) is equal to dataframe2 ($fileName1): $resultTrue")

  }
}
