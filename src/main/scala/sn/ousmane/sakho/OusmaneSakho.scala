package sn.ousmane.sakho

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import sn.ousmane.sakho.services.Utils

import java.io.File

object OusmaneSakho {
  val warehouseLocation = new File("spark-warehouse").getAbsolutePath
  val spark: SparkSession = SparkSession
    .builder()
    .appName("Final Spark App")
    //.config("spark.master", "local")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .getOrCreate()

  def readFile(spark: SparkSession, header: Boolean, delim: String, path: String, format: String) = {
    spark
      .read
      .format(format)
      .option("header", header)
      .option("delimiter", delim)
      .load(path)
  }

  def main(args: Array[String]): Unit = {
    val titanicFilePath = if(args.length > 1) args(0) else "src/data/titanic.csv"
    val countryFilePath = if(args.length > 1) args(1) else "src/data/country.csv"

    val titanicDF: DataFrame = readFile(spark, true, ",", titanicFilePath, "csv")
    titanicDF.show()

    val countryTitanic: DataFrame = readFile(spark, true, ";", countryFilePath, "csv")
    countryTitanic.show()

    titanicDF.printSchema()

    // import org.apache.spark.sql.functions.{col, lit, when}

    val titanicDF2: DataFrame = titanicDF.select("PassengerId", "Name", "Pclass", "Cabin", "Sex", "Age", "Survived", "Parch", "Embarked")
    titanicDF2.show()

    val titanicDF3: DataFrame = titanicDF2.withColumnRenamed("Name", "FullName")
    titanicDF3.show()

    val survivedFemale: DataFrame = titanicDF3.where("Sex == 'female' and Survived == '1'")
    survivedFemale.show()
    val numberSurvivedFemale = survivedFemale.count
    println(s"Le nombre de femmes qui ont survécu sont: $numberSurvivedFemale")


    val titanicDF4: DataFrame = titanicDF3.withColumn("sexletter", substring(col("Sex"), 0, 1))
    titanicDF4.show()


    val distinctPclass: DataFrame = titanicDF4.select(col("Pclass")).distinct()
    distinctPclass.show()

    val titanicDF5: DataFrame = titanicDF4.drop("Parch", "Embarked")
    titanicDF5.show()


    val titanicDF6: DataFrame = titanicDF5.withColumn("isOldEnaugh", when(col("Age") > 18, lit(1)).otherwise(lit(0)))
    titanicDF6.show()

    val titanicDF7: DataFrame = titanicDF6.na.fill("0", Seq("Cabin"))
    titanicDF7.show()

    val survivedBySex = titanicDF7.groupBy("Sex").agg(sum("Survived").name("Survived"), avg("Age").name("Average Age"), count("Survived").name("Total")).withColumn("dead", col("Total") - col("Survived"))
    survivedBySex.show()

    // L'age  et le sexe impacte la survie


    // Calculez le nombre de passagers par Sex qui n'ont pas survécus et ont moins de 18 ans.
    // J'aurai proposé la requete ci-dessous pour répondre à la question.
    var survivedYoungBySex: DataFrame = titanicDF7.where(col("Survived") === 0 && col("isOldEnaugh") === 0).groupBy("Sex").agg(count("*"))
    survivedYoungBySex.show()
    // Pour suivre les Tips
    survivedYoungBySex = titanicDF7.groupBy("Sex", "isOldEnaugh").agg(sum("Survived"))

    survivedYoungBySex.show()

    val dfFinal: DataFrame = countryTitanic.join(titanicDF7, titanicDF7("PassengerId") === countryTitanic("PassengerId"), "inner").drop(titanicDF7("PassengerId"))

    dfFinal.show()


    val titanicDF8 = dfFinal.groupBy("Country").agg(sum("Survived").name("Survived"), count("Survived").name("Total")).withColumn("dead", col("Total") - col("Survived")).sort(col("Survived").desc)
    titanicDF8.show()
    // Argentine et Britannique est le pays avec le plus de survivant 15
    // Britannique et Bouthan sont les pays avec le plus de morts 22

    Utils.writeDataframeinHive(dfFinal, "csv")
  }

}
