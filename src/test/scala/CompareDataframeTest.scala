import CompareRunner.USER_HOME_DIR
import SparkUtilities.{compareDFs, flattenDataFrame}
import org.apache.log4j.Logger
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class CompareDataframeTest extends AnyFunSuite {

  val logger = Logger.getLogger("CompareTestLogger")
  import org.apache.spark.sql.SparkSession

  val spark: SparkSession = SparkSession.builder
    .master("local[*]")
    .appName("CompareDataframes")
    .getOrCreate()

  val sc = spark.sparkContext
  import spark.implicits._

  test("Compare two empty dataframes") {

    val emptyDf1 = spark.emptyDataFrame
    val emptyDf2 = spark.emptyDataFrame

    val result = compareDFs(emptyDf1, emptyDf2)

    withClue("two empty dataframes are equal") {
      result shouldBe true
    }
  }

  test("Compare two same non-empty dataframes") {

    val df1 = Range(1, 1000).toDF
    val df2 = df1

    val result = compareDFs(df1, df2)

    withClue("two same non-empty dataframes are equal") {
      result shouldBe true
    }
  }

  test("Compare two different non-empty dataframes") {

    import org.apache.spark.sql.functions.{lit, when}
    val df1 = Range(1, 1000).toDF
    val df3 = df1.withColumn("value", when($"value" === 1, lit(78)).otherwise($"value"))

    val result = compareDFs(df1, df3)

    withClue("two different non-empty dataframes are not equal") {
      result shouldBe false
    }
  }

  test("Compare two same JSON files as dataframes") {

    val rootPath = s"$USER_HOME_DIR/Repos/my-spark-utilities/src/main/resources"
    val fileName1 = "sample.json"
    val df1 = spark.read.option("multiline", "true").json(s"$rootPath/$fileName1")

    val result = compareDFs(flattenDataFrame(df1, true), flattenDataFrame(df1, true))

    withClue("two same JSON files") {
      result shouldBe true
    }
  }

  test("Compare two different JSON files as dataframes") {

    val rootPath = s"$USER_HOME_DIR/Repos/my-spark-utilities/src/main/resources"
    val fileName1 = "sample.json"
    val fileName2 = "sample2.json"
    val df1 = spark.read.option("multiline", "true").json(s"$rootPath/$fileName1")
    val df2 = spark.read.option("multiline", "true").json(s"$rootPath/$fileName2")

    val result = compareDFs(flattenDataFrame(df1, true), flattenDataFrame(df2, true))

    withClue("two different JSON files") {
      result shouldBe false
    }
  }
}

