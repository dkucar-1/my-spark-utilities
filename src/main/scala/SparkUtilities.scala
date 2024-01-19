import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

object SparkUtilities {

  def flattenDataFrame(df: DataFrame, flattenArray: Boolean = false): DataFrame = {

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
    // USAGE: compareDFs(dataFrame1, dataFrame2)
    // returns: Boolean value

    val df1Cnt = df1.count
    val df2Cnt = df2.count

    // compare schema
    if (df1.schema.toString != df2.schema.toString) {
      println("schemas are different")
      false
    }

    else if (df1Cnt == 0 | df2Cnt == 0) {
      println(s"row count 1 is ${df1Cnt}; row count 2 is ${df2Cnt}")
      df1Cnt == df2Cnt
    }

    // compare row counts
    else if (df1Cnt != df2Cnt) {
      println("row counts are different")
      false
    }

    else {
      // reverse order of index and contents
      val rdd1 = df1.rdd.zipWithIndex.map(r => (r._2, r._1.get(0)))
      val rdd2 = df2.rdd.zipWithIndex.map(r => (r._2, r._1.get(0)))

      //true
      /* reduce by key on the index i.e. r._1
         grab only the value (a boolean)
         then we have a list of booleans
         ensure all booleans are true */

      rdd1.union(rdd2)
        .reduceByKey(_ == _)
        .map(t => t._2.asInstanceOf[Boolean])
        .reduce(_ && _)


    }
  }

  def compareDFsSymDiff(df1: org.apache.spark.sql.DataFrame, df2: org.apache.spark.sql.DataFrame): Boolean = {
    // USAGE: compareDFs(dataFrame1, dataFrame2)
    // returns: Boolean value

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
