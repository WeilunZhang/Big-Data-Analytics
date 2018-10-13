package com.talkingdata.lookalike.data
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.rdd.RDD
//case class TdidFeatDays(tdid:(String,Int),features:(Seq[String], Int))

object updateTdidPerDy_save {
  def getKeyValuesOfEveryFactoryFromParquet(info: DataFrame): RDD[((String, Int), (Set[String], Int))] = {
    info.map(x => {
      val key = (x.getAs[String](0), x.getAs[Int](1))
      val v1 = x.getAs[Map[Long, Integer]](2).keys.map(p => p.toString + "\tapp").toSet
      val v2 = x.getAs[Map[Long, Integer]](3).keys.map(p => p.toString + "\tapp").toSet
      val v3 = x.getAs[Map[Long, Integer]](4).keys.map(p => p.toString + "\tapp").toSet
      val v4 = Set(x.getAs[String](5) + "\tmodel", x.getAs[String](6) + "\tpix")
      val values = v1 ++ v2 ++ v3 ++ v4
      var days = 0
      key -> (values, days)
    })
  }

  def main(args: Array[String]): Unit = {
    val spark = new SQLContext(new SparkContext(new SparkConf().setAppName("updateTdidPerDy_save")))
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    val hdfsPath_ga = "/datascience/etl2/aggregate/ga/2018/"
    val hdfsPath_ta = "/datascience/etl2/aggregate/ta/2018/"
    val hdfsPath_adt = "/datascience/etl2/aggregate/adt/2018/"
    val savePth = "/datalab/test0905/data1/"

    def getDataPerDay(day: String): RDD[((String, Int), (Set[String], Int))] = {
      val firstDayPath_ga = hdfsPath_ga + day
      val firstDayPath_ta = hdfsPath_ta + day
      val firstDayPath_adt = hdfsPath_adt + day
      val path  = List(firstDayPath_ga,firstDayPath_ta,firstDayPath_adt)
      val dataPerDay_ga = spark.read.parquet(firstDayPath_ga).select("deviceId", "platform", "apps.install", "apps.open", "apps.run", "info.model", "info.pix")
      var dataPerDay = getKeyValuesOfEveryFactoryFromParquet(dataPerDay_ga).reduceByKey((a, b) => (a._1 ++ b._1, a._2), 300)
      val dataPerDay_ta = spark.read.parquet(firstDayPath_ta).select("deviceId", "platform", "apps.install", "apps.open", "apps.run", "info.model", "info.pix")
      dataPerDay = dataPerDay.union(getKeyValuesOfEveryFactoryFromParquet(dataPerDay_ta).reduceByKey((a, b) => (a._1 ++ b._1, a._2), 600))
      val dataPerDay_adt = spark.read.parquet(firstDayPath_adt).select("deviceId", "platform", "apps.install", "apps.open", "apps.run", "info.model", "info.pix")
      dataPerDay = dataPerDay.union(getKeyValuesOfEveryFactoryFromParquet(dataPerDay_adt).reduceByKey((a, b) => (a._1 ++ b._1, a._2), 600))
      dataPerDay = dataPerDay.reduceByKey((a, b) => (a._1 ++ b._1, a._2))
//      var dataPerDayDF = spark.read.parquet(firstDayPath_ga).select("deviceId", "platform", "apps.install", "apps.open", "apps.run", "info.model", "info.pix")
//      dataPerDayDF = dataPerDayDF.unionAll(spark.read.parquet(firstDayPath_ta).select("deviceId", "platform", "apps.install", "apps.open", "apps.run", "info.model", "info.pix"))
//      dataPerDayDF = dataPerDayDF.unionAll(spark.read.parquet(firstDayPath_adt).select("deviceId", "platform", "apps.install", "apps.open", "apps.run", "info.model", "info.pix"))
//      val dataPerDay = getKeyValuesOfEveryFactoryFromParquet(dataPerDayDF).repartition(3000).reduceByKey((a, b) => (a._1 ++ b._1, a._2))
      dataPerDay
    }

    val dstpath = new Path(hdfsPath_ga)
    val fsMonths = fileSystem.listStatus(dstpath).filter(p => p.getPath().getName() == "04" ) //&& p.getPath().getName() <= "06"
    for (fsMonth <- fsMonths) {
      val pathMonth_ga = hdfsPath_ga + "/" + fsMonth.getPath().getName()
      val fsDays = fileSystem.listStatus(new Path(pathMonth_ga))
      for (fsDay <- fsDays) {
        val fsMonthDay = fsMonth.getPath().getName + "/" + fsDay.getPath().getName()
        val newPerDay = getDataPerDay(fsMonthDay)
        import spark.implicits._
        val newPerDayDF = newPerDay.repartition(80).map(p => TdidFeatDays(p._1, (p._2._1.toSeq, p._2._2))).toDF()
        newPerDayDF.write.parquet(savePth + fsMonthDay)
      }
    }
  }
}
