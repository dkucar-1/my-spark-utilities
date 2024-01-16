import org.apache.spark.sql.SparkSession
import SparkUtilities.FlattenDataFrame
import org.apache.log4j.Logger

object FlattenRunner {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Test Flatten")
      .getOrCreate()

    val sc = spark.sparkContext // If you need SparkContext object

    val USER_HOME_DIR = System.getProperty("user.home")

  def main(args: Array[String]): Unit = {
      val rootPath = s"$USER_HOME_DIR/scala/SparkUtilities/src/main/resources"
      val fileName = "Jones_1-6.json"
      val df = spark.read.option("multiline", "true").json(s"$rootPath/$fileName")
      val outDf = FlattenDataFrame.flattenDataFrame(df, true)
      val logger = Logger.getLogger("myproj")
      logger.info(outDf.show())
    }
}
