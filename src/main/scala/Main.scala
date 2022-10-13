
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.varia.NullAppender
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, functions}
object Main {
  def main(args: Array[String]): Unit = {
    val nullAppender = new NullAppender
    BasicConfigurator.configure(nullAppender)
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark SQL basic example")
      .getOrCreate()
    //the path of folder contains folder
    val path = "C:\\Users\\Msys\\Desktop\\files\\*";
    //read all the data From file
    val rdd = spark.sparkContext.textFile(path)
    val counts = {
      rdd.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).foreach(f => {
        println(f._1 + "=>" + f._2)
      })
    }
    //print the number of col and row
    val nRows = rdd.count()
    val data = rdd.map(line => line.split(",").map(elem => elem.trim()))
    val nCols = data.collect()(0).length
    println("number of row" + nRows)
    println("number of col" + nCols)

    //create a Data Frame of the files contain 2 coulmn value and file name
    val filterFile: String => String = _.split("/").takeRight(1)(0)
    val filterFileUDF = udf(filterFile);
    val DataFrame = spark.read.format(path).text(path).withColumn("Filename", (filterFileUDF(input_file_name())
      )).withColumn("Value", explode(functions.split(column("value"), " "))).orderBy(column("Filename"))
      .withColumn("Filename", collect_list("Filename").over(Window.partitionBy("Value")))
    println("the value and where are in fileName")
    DataFrame.select("Value","Filename").show()
    DataFrame.select("Value","Filename").orderBy("Value").collect().take(20).foreach(println)
    println("the value and where are in fileName and Count of the word")

    DataFrame.groupBy("Value","Filename").agg(count("Value").as("Count")).orderBy("Value")
      .show()
    println("the value and doc  for “value”, then the system should print: doc3, doc6, doc120, doc133.")
val d1=DataFrame.filter(DataFrame("Value")==="fares").show()
val d2=DataFrame.filter(DataFrame("Value")==="kalboneh").show()
    val d3=DataFrame.filter(DataFrame("Value")==="noor").show()
  }}