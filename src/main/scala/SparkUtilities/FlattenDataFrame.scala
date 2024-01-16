package SparkUtilities

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StructType}

object FlattenDataFrame {

  def flattenDataFrame(df: DataFrame, flattenArray: Boolean = false): DataFrame = {

    def interiorFlatten(df: DataFrame): DataFrame = {

      val fields = df.schema.fields
      val allFields = fields.map(x => x.name)
      for (field <- fields) {
        field.dataType match {
          case arrayType: ArrayType if (flattenArray) =>
            val fieldsExcludingArray = allFields.filter(_ != field.name)
            val fieldsWithExplodedArray = fieldsExcludingArray ++ Array(s"explode(${field.name}) as ${field.name}")
            val explodedDf = df.selectExpr(fieldsWithExplodedArray: _*)
            return interiorFlatten(explodedDf)
          case structType: StructType =>
            val childFieldnames = structType.fieldNames.map(childname => field.name + "." + childname)
            val newfieldNames = allFields.filter(_ != field.name) ++ childFieldnames
            val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString()
              .replace(".", "__")
              .replace("$", "__")
              .replace("-", "minus__")
              .replace("+", "plus__")
              .replace(" ", "")
              .replace("-", ""))))
            val explodedf = df.select(renamedcols: _*)
            return interiorFlatten(explodedf)
          case _ =>
        }
      }
      df
    }

    interiorFlatten(df)
  }
}

