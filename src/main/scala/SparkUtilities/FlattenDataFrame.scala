package SparkUtilities

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.types.{ArrayType, StructType}

object FlattenDataFrame {

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
}

