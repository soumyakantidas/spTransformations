import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Soumyakanti on 24-02-2017.
  */
object MockEPROF {

  def main(args: Array[String]): Unit = {

    val(sc, sqlc) = init
    setLogLevel

    val path = "D:\\global-coding-env\\IdeaProjects\\sps\\src\\main\\resources"

    val rows = sc.makeRDD(getRows(numOfProfiles = 100, numOfDates = 10))

    val uniqueRows = makeUnique(rows)

    val columns = ("PROFILE,VALUEDAY," + (0 to 47).map(getColumnName).mkString(",")).split(",")

    val schema = StructType(columns.map(col => {
      StructField(col, StringType, nullable = true)
    }))

    val dataDF = sqlc.createDataFrame(uniqueRows, schema)

    dataDF.write.format("com.databricks.spark.csv").option("header", "true").save(path + "\\MockEPROF.csv")

//    dataDF.groupBy("profile", "date").count().show(10)


//    uniqueRows.saveAsTextFile(path + "\\MockEPROF.csv")

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

  private def getRows(numOfProfiles: Int, numOfDates: Int) = {

    val numOfRows = numOfProfiles * numOfDates

    val profiles = (0 until numOfProfiles).map(n => {
      (n, "profile" + n)
    }).toMap
    val dates = (0 until numOfDates).map(n => {
      (n, "date" + n)
    }).toMap
    val random = scala.util.Random

    val rows = (1 to numOfRows).map(_ => {
      ((profiles.get(random.nextInt(numOfProfiles)).get,
      dates.get(random.nextInt(numOfDates)).get),
        (1 to 48).map(_ => random.nextInt(100)).toArray)
    })

    rows
  }

  private def makeUnique(rows: RDD[((String, String), Array[Int])]): RDD[Row] /*RDD[((String, String), Array[Int])]*/ ={
    val uniqueRows = rows
      .reduceByKey((tup1, tup2) => {
        val arr = tup1.zip(tup2).map{
          case (a, b) => a + b
        }
        arr
      })
      .map(row => {
        Row.fromSeq(Array(row._1._1, row._1._2, row._2.mkString(",")).mkString(",").split(","))
      })

    uniqueRows
  }

  private def getColumnName(n: Int): String = {
    val prefix = "VAL"
    val hour = if (n/2 < 10) "0" + (n/2).toString else (n/2).toString
    val min = if (n%2 == 0) "00" else "30"

    prefix + hour + min
  }

}
