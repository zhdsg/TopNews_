package com.eastday.spark



import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import com.eastday.domain.{NewsDomain, AppFlowDomain, ActiveLog, PagePosition}
import com.eastday.utils.{ETLUtil, DateUtil}
import dao.Impl.AppFlowInsertDaoImpl
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.{SQLContext, Row, Dataset, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

import scala.util.Try

/**
 * Created by admin on 2018/6/22.
 */
object AppFlow {
  def main(args: Array[String]) {
    if(args.length==0){
      println("please input params........")
      System.exit(-1)
    }
//    val dateStr :StringBuffer=new StringBuffer("")
//    dateStr.append(DateUtil.getFormatTime(args(0)))


    //spark 具体句柄创建
    val conf: SparkConf = new SparkConf()
      //.setAppName("xiaochengxu_"+args(0).substring(0,8)+"_"+args(0).substring(8,12))
      .setAppName(s"rate2mysql_${args(0)}")
      .set(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES
        ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_CONSOLIDATEFILES))
      .set(Constract.SPARK_SHUFFLE_FILE_BUFFER
        ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_FILE_BUFFER))
      .set(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT
        ,ConfigurationManager.getString(Constract.SPARK_REDUCER_MAXSIZEINFLIGHT))
      .set(Constract.SPARK_SHUFFLE_IO_MAXRETRIES
        ,ConfigurationManager.getString(Constract.SPARK_SHUFFLE_IO_MAXRETRIES))
//      .set(Constract.SPARK_DEFAULT_PARALLELISM
//        ,ConfigurationManager.getString(Constract.SPARK_DEFAULT_PARALLELISM))
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .registerKryoClasses(Array(classOf[NewsDomain],classOf[AppFlowDomain]))


    val sc  =new SparkContext(conf)

    val spark = new SQLContext(sc)

    import spark.implicits._

    try{
      val fs =FileSystem.get(new Configuration())
      val arg =args(0)
      val dt =arg.substring(0,8)
      val hour =arg.substring(8,10)
      println(dt+" " +hour)
      val dt_ago = DateUtil.getXdaysAgo(dt,4)
      val data0: DataFrame = getDataSource4AppFlow(sc,dt,hour,fs).toDF("dt","hour","url","rate")
      //data0.show()
      data0.registerTempTable("tab0")
      val data1 = getDataSource4Video(sc,dt_ago,dt,fs).toDF()
     // println(data1.count())
      // data1.show()
      data1.registerTempTable("tab1")
      val data2 = getDataSource4Crawler(sc,dt_ago,dt,fs).toDF()
     // println(data2.count())
      // data2.show()
      data2.registerTempTable("tab2")
      val data3 =getDataSource4ShortVideo(sc,dt_ago,dt,fs).toDF()
      //println(data3.count())
      //data3.show()
      data3.registerTempTable("tab3")
//      val sql_PagePositionHistory =
//        s"""
//          |select substring(a.dt,0,8) as dt ,substring(a.dt,9,10) as hour ,
//          |case when c.urlid is not null then c.type1
//          | when b.urlid is not null then b.tppy
//          | else d.urlid end as type1 ,
//          |case when c.urlid is not null then c.title
//          | when b.urlid is not null then b.title
//          | else d.title end as title ,
//          |case when c.urlid is not null then c.url
//          | when b.urlid is not null then b.htmlurl
//          | else d.url end as url ,a.rate
//          |from (
//          |select url ,rating ,dt  from
//          |ctrnews_biasMF.minwap_app_flow_show_active_result
//          |where dt =${dt}${hour}
//          |)a
//          |left outer join (
//          |select regexp_extract(htmlurl,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2) as urlid ,htmlurl  ,title,tppy
//          |from minieastdaywap.rpt_video_baseinfo_uniq_new where dt > ${dt_ago} and dt<=${dt}
//          |) b
//          |on a.url = b.urlid
//          |left outer join
//          |(
//          |select regexp_extract(url,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2) as urlid ,url  ,title,type1
//          |from minieastdaywap.log_crawler_createhtml where dt > ${dt_ago} and dt<=${dt}
//          |)
//          | c
//          |on a.url = c.urlid
//          |left outer join
//          |(select regexp_extract(url,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2) as urlid ,url  ,title,typepy
//          | from minieastdaywap.log_short_video_createhtml where dt > ${dt_ago} and dt<=${dt}
//          | )d
//          |on a.url = d.urlid
//          |
//        """.stripMargin
      val sql_PagePositionHistory =
        """
           |select a.dt , a.hour ,
           |case when c.urlid is not null then c.typepy
           | when b.urlid is not null then b.typepy
           | else d.typepy end as type1 ,
           |case when c.urlid is not null then c.title
           | when b.urlid is not null then b.title
           | else d.title end as title ,
           |case when c.urlid is not null then c.url
           | when b.urlid is not null then b.url
           | when d.urlid is  not null and  b.urlid is null  then d.url
           | else a.url  end as url ,a.rate
           |from ( select url ,rate ,dt,hour  from  tab0 )a
           |left outer join
           |tab1 b
           |on a.url = b.urlid
           |left outer join
           |tab2 c
           |on a.url = c.urlid
           |left outer join
           |tab3 d
           |on a.url = d.urlid
         """.stripMargin

      val data_PagePositionHistory: Dataset[AppFlowDomain] = spark.sql(sql_PagePositionHistory).as[AppFlowDomain]

      //data_PagePositionHistory.show()
      val appFlowInsertDao =new AppFlowInsertDaoImpl
      appFlowInsertDao.del(dt,hour)
      data_PagePositionHistory.rdd.foreachPartition(iter=>{
        val appFlowInsertDao =new AppFlowInsertDaoImpl
        iter.foreach(f=>
          appFlowInsertDao.insert(f)
        )
      })
    }catch{
      case e :Exception => {
        e.printStackTrace()
        sc.stop()
        //spark.stop()
        System.exit(-1)

      }
    }finally{
      sc.stop()
      //spark.stop()
    }


    sc.stop()

  }


  def getDataSource4Video(sc : SparkContext ,dt_ago :String ,dt:String,fs :FileSystem):RDD[NewsDomain]={

    var data: RDD[String] = sc.parallelize(Nil)
    val path ="hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/rpt_video_baseinfo_uniq_new"
    for ( dt_curr <- dt_ago.toLong to dt.toLong){

      val curr_path =s"${path}/dt=${dt_curr}"
      println(curr_path)
      if(fs.exists(new Path(curr_path))){
       val data_temp: RDD[String] = sc.textFile(curr_path)
        data = data_temp.union(data)
      }

    }
    //println(data.count())
    data.map(f=>{
      val spilt =f.split("\t")
      //(urlid,htmlurl,title,tppy)
      Try(ETLUtil.regexpExtract(spilt(23),"(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)",2),spilt(23),spilt(12),spilt(7))
    }).filter(_.isSuccess).map(f=>{
      val row =f.get

      NewsDomain(row._3,row._4,row._2,row._1)
    })
  }


  def getDataSource4Crawler(sc : SparkContext ,dt_ago :String ,dt:String,fs :FileSystem):RDD[NewsDomain]={

    var data: RDD[String] = sc.parallelize(Nil)
    val path ="hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/log_crawler_createhtml"
    for ( dt_curr <- dt_ago.toLong to dt.toLong){

      val curr_path =s"${path}/dt=${dt_curr}"
      println(curr_path)
      if(fs.exists(new Path(curr_path))){
        val data_temp: RDD[String] = sc.textFile(curr_path)
        data = data_temp.union(data)
      }

    }
   // println(data.count())
    data.map(f=>{
      val spilt =f.split("\t")
      //(urlid,url,title,type1)
      Try(ETLUtil.regexpExtract(spilt(1),"(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)",2),spilt(1),spilt(2),spilt(9))
    }).filter(_.isSuccess).map(f=>{
      val row =f.get

      NewsDomain(row._3,row._4,row._2,row._1)
    })
  }

  def getDataSource4ShortVideo(sc : SparkContext ,dt_ago :String ,dt:String,fs :FileSystem):RDD[NewsDomain]={

    var data: RDD[String] = sc.parallelize(Nil)
    val path ="hdfs://Ucluster/user/hive/warehouse/minieastdaywap.db/log_short_video_createhtml"
    for ( dt_curr <- dt_ago.toLong to dt.toLong){

      val curr_path =s"${path}/dt=${dt_curr}"
      println(curr_path)
      if(fs.exists(new Path(curr_path))){
        val data_temp: RDD[String] = sc.textFile(curr_path)
        data = data_temp.union(data)
      }

    }
   // println(data.count())
    data.map(f=>{
      val spilt =f.split("\t")
      //(urlid,url,title,typepy)
      Try(ETLUtil.regexpExtract(spilt(6),"(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)",2),spilt(6),spilt(4),spilt(9))
    }).filter(_.isSuccess).map(f=>{
      val row =f.get

      NewsDomain(row._3,row._4,row._2,row._1)
    })
  }



  def getDataSource4AppFlow(sc : SparkContext ,dt :String,hour:String ,fs :FileSystem):RDD[(String,String,String,Double)]={

    val path =s"hdfs://Ucluster/user/hive/warehouse/ctrnews_biasmf.db/minwap_app_flow_show_active_result/dt=${dt}${hour}"

    println(path)

    val data: RDD[String] = sc.textFile(path)

    data.map(f=>{
      val spilt =f.split("\t")
      //(dt,hour,url,rating)
      Try(dt,hour,spilt(0),spilt(1).toDouble)
    }).filter(_.isSuccess).map(_.get)
  }
}
