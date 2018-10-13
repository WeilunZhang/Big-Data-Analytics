package com.talkingdata.lookalike.data
//第4版
// 利用bloomfilter从总的数据中选出需要更新的数据，再对这些数据mapreduce更新操作
import org.apache.hadoop.fs._
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SQLContext}
import scala.util.hashing.MurmurHash3
import bloomfilter.mutable.BloomFilter
//import breeze.util.BloomFilter
case class TdidDays(tdid:String, days: Int)

object updateTdidPerDay4 {
  def hash(packageName: String): Long = {
    var h = 112589990L
    var i = 0
    while ( {
      i < packageName.length
    }) {
      h = 62 * h + packageName.charAt(i)
      h = h & Long.MaxValue

      {
        i += 1; i - 1
      }
    }
    if (h == Long.MaxValue) h = h - 1
    h
  }


  def getKeyValuesOfEveryFactoryFromParquet(info: DataFrame) = {
    info.map(x => {
      val tdid = x.getAs[String](0)
      tdid -> 0
    })
  }
  def getDataPerDay(day: String,spark:SQLContext,hdfsPath_ga:String,hdfsPath_ta:String,hdfsPath_adt:String) = {
    val firstDayPath_ga = hdfsPath_ga + day
    val firstDayPath_ta = hdfsPath_ta + day
    val firstDayPath_adt = hdfsPath_adt + day
    spark.read.parquet(firstDayPath_ga,firstDayPath_ta,firstDayPath_adt).select("deviceId").distinct()
  }
  val tdid_num = 250000000
  def main(args: Array[String]): Unit = {
    val spark = new SQLContext(new SparkContext(new SparkConf().setAppName("updateTdidPerDay4")))
    val conf = new Configuration()
    val fileSystem = FileSystem.get(conf)
    val hdfsPath_ga = "/datascience/etl2/aggregate/ga/2018/"
    val hdfsPath_ta = "/datascience/etl2/aggregate/ta/2018/"
    val hdfsPath_adt = "/datascience/etl2/aggregate/adt/2018/"
    val Falseposi2 =  spark.sparkContext.broadcast(args(0).toDouble)
    val dstpath = new Path(hdfsPath_ga)
    val fsMonths = fileSystem.listStatus(dstpath).filter(p => p.getPath().getName() == "04")// && p.getPath().getName() <= "06"
    var tdidALL = getKeyValuesOfEveryFactoryFromParquet(getDataPerDay("04/01",spark,hdfsPath_ga,hdfsPath_ta,hdfsPath_adt))

    for (fsMonth <- fsMonths) {
      val pathMonth_ga = hdfsPath_ga + "/" + fsMonth.getPath().getName()
      val pathMonth_ta = hdfsPath_ta + "/" + fsMonth.getPath().getName()
      val pathMonth_adt = hdfsPath_adt + "/" + fsMonth.getPath().getName()
      val fsDays = fileSystem.listStatus(new Path(pathMonth_ga)).filter(p => p.getPath().getName() <= "03")
      for (fsDay <- fsDays) {
        val month = fsMonth.getPath().getName()
        val day = fsDay.getPath().getName()
        val fsMonthDay =  month + "/" + day
        val newPerDayDF = if(!fileSystem.exists(new Path("/datalab/test0905/"+fsMonthDay))){
          val result = getDataPerDay(fsMonthDay,spark,hdfsPath_ga,hdfsPath_ta,hdfsPath_adt).persist()
          result.repartition(40).write.parquet("/datalab/test0905/"+fsMonthDay)
          result
        }
        else {
          spark.read.parquet("/datalab/test0905/" + fsMonthDay)
        }
//        val num_count = newPerDayDF.count()
//        val Num_args1 = spark.sparkContext.broadcast(num_count)
        val newPerDay = getKeyValuesOfEveryFactoryFromParquet(newPerDayDF)
        val bf = BloomFilter[String](tdid_num, Falseposi2.value)
        newPerDayDF.collect().foreach(tdid=>bf.add(tdid.getAs[String](0)))
//        val bfAllBroadcast = spark.sparkContext.broadcast(bf5)
        // 每天的数据做bloomfilter
        val tdidfeatsALL_toReduce = tdidALL.filter(p=>{bf.mightContain(p._1)})
        val tdidfeatsALL_last_upated = tdidALL.filter(p=>{bf.mightContain(p._1)})
        val tdidfeatsALL_Reduced = newPerDay.union(tdidfeatsALL_toReduce).reduceByKey((_,_)=>0)
        tdidALL = tdidfeatsALL_last_upated.union(tdidfeatsALL_Reduced).map(p=>(p._1, p._2+1)).repartition(500)
      }
    }
    import spark.implicits._
    tdidALL.map(p => TdidDays(p._1, p._2)).repartition(100).toDF().write.parquet("/datalab/test0905/test001")
  }
}
