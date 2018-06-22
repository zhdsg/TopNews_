package com.eastday.spark



import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import com.eastday.domain.{AppFlowDomain, ActiveLog, PagePosition}
import com.eastday.utils.{ETLUtil, DateUtil}
import dao.Impl.AppFlowInsertDaoImpl

import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkContext, SparkConf}

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
      .setAppName(s"installAggr_${args(0)}")
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
    //.registerKryoClasses()

    //        .setMaster("local")
    //      .set("spark.testing.memory","471859200")
    //      .set("spark.sql.warehouse.dir","file:////F://workspace//TopNews")
    //      .set("spark.driver.memory","5g")
    //val sc: SparkContext = SparkContext.getOrCreate(conf)
    val sc  =new SparkContext(conf)

    //设置读取时长
    //    sc.hadoopConfiguration.set(Constract.DFS_CLIENT_SOCKET_TIMEOUT
    //      ,ConfigurationManager.getString(Constract.DFS_CLIENT_SOCKET_TIMEOUT))
    val spark = new HiveContext(sc)
    //    val spark: SparkSession = SparkSession.builder()
    //      .config(conf)
    //      .enableHiveSupport()
    //      .getOrCreate()
    import spark.implicits._

    try{
      val dt =args(0).substring(0,8)
      val hour =args(0).substring(9,10)

      val dt_ago = DateUtil.getXdaysAgo(dt,4)
      val sql_PagePositionHistory =
        s"""
          |select substring(a.dt,0,8) as dt ,substring(a.dt,9,10) as hour ,
          |case when c.urlid is not null then c.type1
          | when b.urlid is not null then b.tppy
          | else d.urlid end as type1 ,
          |case when c.urlid is not null then c.title
          | when b.urlid is not null then b.title
          | else d.title end as title ,
          |case when c.urlid is not null then c.url
          | when b.urlid is not null then b.htmlurl
          | else d.url end as url ,a.rate
          |from (
          |select url ,rating ,dt  from
          |ctrnews_biasMF.minwap_app_flow_show_active_result
          |where dt =${dt}${hour}
          |)a
          |left outer join (
          |select regexp_extract(htmlurl,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2) as urlid ,htmlurl  ,title,tppy
          |from minieastdaywap.rpt_video_baseinfo_uniq_new where dt > ${dt_ago} and dt<=${dt}
          |) b
          |on a.url = b.urlid
          |left outer join
          |(
          |select regexp_extract(url,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2) as urlid ,url  ,title,type1
          |from minieastdaywap.log_crawler_createhtml where dt > ${dt_ago} and dt<=${dt}
          |)
          | c
          |on a.url = c.urlid
          |left outer join
          |(select regexp_extract(url,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2) as urlid ,url  ,title,typepy
          | from minieastdaywap.log_short_video_createhtml where dt > ${dt_ago} and dt<=${dt}
          | )d
          |on a.url = d.urlid
          |
        """.stripMargin


      val data_PagePositionHistory: Dataset[AppFlowDomain] = spark.sql(sql_PagePositionHistory).as[AppFlowDomain]
      val appFlowInsertDao =new AppFlowInsertDaoImpl
      appFlowInsertDao.del(dt,hour)
      data_PagePositionHistory.rdd.foreachPartition(iter=>{
        val appFlowInsertDao =new AppFlowInsertDaoImpl
        iter.map(f=>
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




  }





}
