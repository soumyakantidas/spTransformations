import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by Soumyakanti on 25-02-2017.
  */
object chnageTZ {

  def main(args: Array[String]): Unit = {

    val(sc, sqlc) = init
    setLogLevel()

    val dataDF = sqlc.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("D:\\global-coding-env\\IdeaProjects\\sps\\src\\main\\resources\\MockEPROF.csv")

    val columns = dataDF.columns

    val columns0to1600 = columns.slice(0, columns.indexOf("VAL1600"))
    val columnsAfter1600 = columns.slice(0, 2) ++ columns.slice(columns.indexOf("VAL1600"), columns.length)

    /*columns0to1600.foreach(println)
    columnsAfter1600.foreach(println)*/

    val h0toh15DF = dataDF.select(columns0to1600.map(col) :_*)
    val h1530TillEndDF = dataDF.select(columnsAfter1600.map(col) :_*)

    val rdd08to2330 = h0toh15DF.map(row => {
      val rowArr = row.mkString(",").split(",")
      val arrOfZeroes = Array.fill[String](16)("0")
//      ( (rowArr(0), rowArr(1)), (arrOfZeroes ++ rowArr.slice(2, rowArr.length)).mkString(",") )
      ( (rowArr(0), rowArr(1)), (2, arrOfZeroes, rowArr.slice(2, rowArr.length)) )
    })
      .sortByKey(ascending = true)

    val rdd00to07 = h1530TillEndDF.map(row => {
      val rowArr = row.mkString(",").split(",")
      val arrOfZeroes = Array.fill[String](32)("0")
//      ( (rowArr(0), getNextDate(rowArr(1))), (rowArr.slice(2, rowArr.length) ++ arrOfZeroes).mkString(",") )
      ( (rowArr(0), getNextDate(rowArr(1))), (1, rowArr.slice(2, rowArr.length), arrOfZeroes) )
    })
      .sortByKey(ascending = true)


//    h0toh15DF.show()
//    rdd08to2330.take(20).foreach(println)
//    h1530TillEndDF.show()
//    rdd00to07.take(20).foreach(println)

    val unionRDD = rdd08to2330.union(rdd00to07)

    val finalRDD = unionRDD
      .reduceByKey((tup1, tup2) => {
        val (arr1, arr2) = if (tup1._1 == 1) {(tup1._2, tup2._3)}else{(tup2._2, tup1._3)}
        (0, arr1, arr2)
      })
      .map(tup => {
        Row.fromSeq((tup._1._1 + "," + tup._1._2 + "," + (tup._2._2 ++ tup._2._3).mkString(",")).split(","))
      })
//      .sortByKey(ascending = true)

    val schema = StructType(columns.map(col => {
      StructField(col, StringType, nullable = true)
    }))

    val finalDF = sqlc.createDataFrame(finalRDD, schema)

    finalDF.sort("PROFILE", "VALUEDAY").show()

  }

  private def init: (SparkContext, SQLContext) = {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("SPSTest")

    val sc = new SparkContext(conf)
    val sqlc = new SQLContext(sc)

    (sc, sqlc)
  }

  private def setLogLevel() = Logger.getRootLogger.setLevel(Level.ERROR)

  private def getNextDate(date: String): String = {
    val num = date.last.toString.toInt + 1
    "date" + num
  }

}
