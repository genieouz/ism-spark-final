package sn.ousmane.sakho
package services

import org.apache.spark.sql.DataFrame

object Utils {
  def writeDataframeinHive(df : DataFrame, format: String): Unit = {
    df.write.mode("overwrite").saveAsTable("ism_m2_2022_examspark.ousmanesakho")
  }
}
