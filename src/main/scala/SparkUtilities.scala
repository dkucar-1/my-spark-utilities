import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

object SparkUtilities {

  def flattenDataFrame(df: DataFrame, flattenArray: Boolean = false): DataFrame = {
    /** Flatten a Spark dataframe that may contain structs or arrays
     *
     *  @param df: org.apache.spark.sql.DataFrame     : dataframe
     *  @param flattenArray: Boolean    : should we explode/flatten arrays as well.. default false
     *  @return a flattened dataframe
     */
    def interiorFlatten(df: DataFrame): DataFrame = {

      val fields = df.schema.fields
      val allFields = fields.map(x => x.name)
      for (field <- fields) {
        field.dataType match {
          case arrayType: ArrayType if (flattenArray) =>
            return interiorFlatten(df.withColumn(s"${field.name}", explode(col(field.name))))
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname => field.name + "." + childname)
            val newfieldNames = allFields.filter(_ != field.name) ++ childFieldnames
            val renamedcols = newfieldNames.map(f => (col(f).as(f
              .replace(".", "__")
              .replace("$", "__")
              .replace("-", "minus__")
              .replace("+", "plus__")
              .replace(" ", "")
              .replace("-", ""))))
            return interiorFlatten(df.select(renamedcols: _*))
          case _ =>
        }
      }
      df
    }

    interiorFlatten(df)
  }

  def compareDFs(df1: org.apache.spark.sql.DataFrame, df2: org.apache.spark.sql.DataFrame): Boolean = {
    /** Given two Spark dataframes, verify if they are equal or not
     *
     *  @param df1: org.apache.spark.sql.DataFrame     : first dataframe
     *  @param df2: org.apache.spark.sql.DataFrame     : second dataframe
     *  @return Boolean indicating equality or not
     */

    def toRdd(df: org.apache.spark.sql.DataFrame) = {
      if (df.isEmpty) None
      else Some(df.rdd.zipWithIndex.map(r => (r._2, r._1.get(0))))
    }

    val rdd1 = toRdd(df1)
    val rdd2 = toRdd(df2)

    !(rdd2.isDefined ^ rdd1.isDefined) &&
      ((rdd1.isEmpty && rdd2.isEmpty) ||
        (rdd1.getOrElse(0).asInstanceOf[org.apache.spark.rdd.RDD[(Long, Any)]]
          .union(rdd2.getOrElse(0).asInstanceOf[org.apache.spark.rdd.RDD[(Long, Any)]])
          .reduceByKey(_ == _)
          .map(t => t._2.asInstanceOf[Boolean])
          .reduce(_ && _)))
  }

  def compareDFsSymDiff(df1: org.apache.spark.sql.DataFrame, df2: org.apache.spark.sql.DataFrame): Boolean = {
    /** Given two Spark dataframes, verify if they are equal or not
     * uses Symmetric difference between two dataframes
     * may not be suitable for wide (many column) or large dataframes
     *
     *  @param df1: org.apache.spark.sql.DataFrame     : first dataframe
     *  @param df2: org.apache.spark.sql.DataFrame     : second dataframe
     *  @return Boolean indicating equality or not
     */

    if (df1.schema.toString != df2.schema.toString) {
      println("schemas are different")
      false
    }
    // step 3: compare row counts
    else if (df1.count != df2.count) {
      println("row counts are different")
      false
    }

    // verify that the symmetric difference between df1 and df2 is empty
    else
      ((df2.exceptAll(df1)).isEmpty) && ((df1.exceptAll(df2)).isEmpty)
  }
}
