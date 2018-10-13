//package com.talkingdata.lookalike.data
//// 利用spark sql中的bloomfilter，利用sql过滤而不是rdd
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs._
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SparkSession
//
//import scala.util.Random
//
//object updateBySQL {
//  case class TdidFeatDays(tdid:String, days:Int)
//  val dataPath = "/datalab/test0831/data/"
//  val hdfsPath_ga = "/datascience/etl2/aggregate/ga/2018/"
//  val hdfsPath_ta = "/datascience/etl2/aggregate/ta/2018/"
//  val hdfsPath_adt = "/datascience/etl2/aggregate/adt/2018/"
//
//  def main(args: Array[String]): Unit = {
//    val sc = new SparkContext(new SparkConf().setAppName("updateBySQL"))
//    val conf = new Configuration()
//    val fileSystem = FileSystem.get(conf)
//    val spark=SparkSession.builder().getOrCreate()
//    val Falseposi2 =  spark.sparkContext.broadcast(args(0).toDouble)
//    def getDataPerDay(day: String) = {
//      val firstDayPath_ga = hdfsPath_ga + day
//      val firstDayPath_ta = hdfsPath_ta + day
//      val firstDayPath_adt = hdfsPath_adt + day
//      var dataPerDayDF = spark.read.parquet(firstDayPath_ga).select("deviceId", "platform")
//      dataPerDayDF = dataPerDayDF.union(spark.read.parquet(firstDayPath_ta).select("deviceId", "platform"))
//      dataPerDayDF = dataPerDayDF.union(spark.read.parquet(firstDayPath_adt).select("deviceId", "platform")).distinct()
//      dataPerDayDF = dataPerDayDF.withColumn("days", dataPerDayDF("platform") * 0)
//      dataPerDayDF
//    }
//
//    val fsMonths = fileSystem.listStatus(new Path(dataPath))
//    var tdidALL = getDataPerDay("04/01")
//    for (fsMonth <- fsMonths) {
//      val pathMonth = dataPath + fsMonth.getPath().getName()
//      val fsDays = fileSystem.listStatus(new Path(pathMonth)).filter(p => p.getPath().getName() <= "02")
//      for (fsDay <- fsDays) {
//        val fsMonthDay = fsMonth.getPath().getName + "/" + fsDay.getPath().getName()
//        var newPerDay = getDataPerDay(fsMonthDay)
//        val Num_args1 = spark.sparkContext.broadcast(newPerDay.count())
//        val dataBF = newPerDay.stat.bloomFilter("tdid", Num_args1.value, Falseposi2.value)
//        val bfBc = sc.broadcast(dataBF) // 广播变量
//        val tdidALL_toReduce = tdidALL.filter(row=>{bfBc.value.mightContainString(row.getString(0))})
//        val tdidALL_last_upated = tdidALL.filter(row=>{bfBc.value.mightContainString(row.getString(0))})
//
//        var tdidALL_Reduced = newPerDay.union(tdidALL_toReduce).select("deviceId", "platform").distinct()
//        tdidALL_Reduced = tdidALL_Reduced.withColumn("days", tdidALL_Reduced("platform") * 0)
//        val tdidALL_dataset = tdidALL_last_upated.union(tdidALL_Reduced)
//        import spark.implicits._
//        tdidALL = tdidALL_dataset.select($"days"+1).withColumnRenamed("(days + 1)", "days")
//        tdidALL.show(50,false)
//      }
//    }
////    import spark.implicits._
////    tdidfeatsALL.map(p => TdidFeatDays(p._1, (p._2._1.toSeq, p._2._2))).repartition(300).toDF().write.parquet("/datalab/test0905/test1")
//  }
//}
