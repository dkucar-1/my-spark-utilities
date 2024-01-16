import org.apache.spark.sql.SparkSession
import SparkUtilities.FlattenDataFrame.flattenDataFrame
import org.apache.log4j.Logger

object FlattenRunner {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Test Flatten")
      .getOrCreate()

    val sc = spark.sparkContext // If you need SparkContext object

    val USER_HOME_DIR = System.getProperty("user.home")

    def main(args: Array[String]): Unit = {
        val logger = Logger.getLogger("myproj")
        val rootPath = s"$USER_HOME_DIR/Repos/my-spark-utilities/src/main/resources"
        val fileName = "sample.json"
        val df = spark.read.option("multiline", "true").json(s"$rootPath/$fileName")
        val outDf = flattenDataFrame(df, true)
        logger.info(outDf.show())
        logger.info(outDf.printSchema)
    }
}
