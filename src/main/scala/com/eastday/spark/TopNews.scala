package com.eastday.spark


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random}

import com.eastday.conf.ConfigurationManager
import com.eastday.constract.Constract
import com.eastday.domain.{ActiveLog, PagePosition}
import com.eastday.utils.{ETLUtil, DateUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StructField, StructType}


import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.{SparkContext, SparkConf}


import scala.collection.mutable.ArrayBuffer


/**
 * Created by admin on 2018/6/8.
 */
object TopNews {
  def main(args: Array[String]) {
    if(args.length==0){
      println("please input params........")
      System.exit(-1)
    }
    val dateStr :StringBuffer=new StringBuffer("")
    dateStr.append(DateUtil.getFormatTime(args(0)))


    //spark 具体句柄创建
    val conf: SparkConf = new SparkConf()
      //.setAppName("xiaochengxu_"+args(0).substring(0,8)+"_"+args(0).substring(8,12))
      .setAppName("demo")
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
      .registerKryoClasses(Array(classOf[PagePosition],classOf[ActiveLog]))

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
    // val broadcast :Broadcast[String] =sc.broadcast(dateStr.toString)

//    val upLine :Long=DateUtil.str2Date(DateUtil.trimDate(dateStr.toString)).getTime/1000
//    val downLine:Long =upLine-600
//
//    val dateTime =DateUtil.trimDate(dateStr.toString)
//    val dateLineUp =upLine
//    var dateLineDown=DateUtil.getZeroTime(dateStr.toString()).getTime/1000
    var dt =DateUtil.getTodayDate(dateStr.toString)
//    if (dateLineDown ==dateLineUp){
//      dateLineDown =dateLineDown-86400
//      dt =DateUtil.getYesterdayDate(dateStr.toString)
//    }
    //val dt="20180612"
    try{



      val properties =new Properties()
      properties.put("user","data_import")
      properties.put("password", "Import20170105")
      val url ="jdbc:mysql://10.9.110.154:3306/miniwaptools?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=round&transformedBitIsBoolean=true"

      val pagePositionHistory: DataFrame =spark.read.jdbc(url,"page_position_history",properties)
      pagePositionHistory.registerTempTable("page_position_history")

      val newsPositionToVideoDataHistory=spark.read.jdbc(url,"news_position_to_video_data_history",properties)
      newsPositionToVideoDataHistory.registerTempTable("news_position_to_video_data_history")

      val configTypeId=spark.read.jdbc(url,"config_type_id",properties)
      configTypeId.registerTempTable("config_type_id")

      val sql_PagePositionHistory =
        """
          |SELECT id,optype,content,title,newstype,idx,pagenum,createuser,modifydate,
          |   starttime,endtime,sourcetype, '' as real_starttime,'' as real_endtime,'' as typeid,'' as real_startdate,'' as real_enddate,'' as typename
          |FROM page_position_history
          |WHERE sourcetype IN ('news','topic','live','video') and apptypeid='DFTT'
          |
        """.stripMargin


      val data_PagePositionHistory: Dataset[PagePosition] = spark.sql(sql_PagePositionHistory).as[PagePosition]

      val spiltPagePositionHistory = spilt4NewTypes(data_PagePositionHistory).toDS()


      val sql_newsPositionToVideoDataHistory =
        """
          |SELECT id,optype,content,title,newstype,idx,pagenum,
          |   createuser,modifydate,starttime,endtime,sourcetype,
          |   '' as real_starttime,'' as real_endtime,'' as typeid,'' as real_startdate, '' as real_enddate ,'' as typename
          |FROM news_position_to_video_data_history
          |WHERE sourcetype IN ('video','live') and apptypeid='DFTT'
          |
        """.stripMargin


      val data_newsPositionToVideoDataHistory: Dataset[PagePosition] = spark.sql(sql_newsPositionToVideoDataHistory).as[PagePosition]

      val spilt_newsPositionToVideoDataHistory = spilt4NewTypes(data_newsPositionToVideoDataHistory).toDS()


      val unionDF =spilt_newsPositionToVideoDataHistory.union(spiltPagePositionHistory)
        .filter(f=> !f.optype.equals("delete")).toDF()

      unionDF.registerTempTable("position_table")

      val sql_joinConfigTypeId=
        s"""
          |
          |SELECT case when n.typename is not null then n.typename else m.typeid end  AS typename,
          |m.typeid,m.id,m.optype, substring_index(m.content,'?',1) AS content,m.title,m.newstype,m.idx,
          | m.pagenum,m.createuser,m.modifydate,m.starttime,m.endtime,sourcetype,
          | real_starttime,real_endtime, real_startdate, real_enddate
          |from position_table m
          |LEFT JOIN config_type_id AS n
          |            ON m.typeid=n.typeid
          |            WHERE m.real_startdate ='${dt}'
        """.stripMargin

      val PagePositionFilter =spark.sql(sql_joinConfigTypeId).as[PagePosition]
      PagePositionFilter.toDF().registerTempTable("mysql_result_temp")

      //      println(PagePositionFilter.show(1000))
      spark.sql("use minieastdaywap")

//      spark.sql(
//        """
//          |insert overwrite table `minieastdaywap`.`mysql_result`
//          |select typename ,typeid ,id,optype,content,title,newstype,idx,
//          |pagenum,createuser,modifydate,starttime,endtime,sourcetype,
//          |real_starttime,real_endtime,real_startdate,real_enddate
//          | from mysql_result_temp
//        """.stripMargin )

//      val  sql_live =s" select roomkey ,id  from dflive_room_base_info where dt='${dt}' "
//      val dfliveRoomBaseInfo =spark.sql(sql_live)
//      dfliveRoomBaseInfo.createOrReplaceTempView("dflive_room_base_info")
      val sql_fetchId =
        s"""
          |select modifydate,createuser,typename,newstype,title,idx,pagenum,
          |unix_timestamp(real_starttime,'yyyy-MM-dd HH:mm:ss') as real_starttime,
          |unix_timestamp(real_endtime,'yyyy-MM-dd HH:mm:ss') as real_endtime,
          |case when  b.flvurl is not  null  then b.flvurl
          |else regexp_extract(a.content,'(.*?)([a-zA-Z]{0,1}[0-9]{15,})(.html[\\?]{0,1}.*)',2) end as content,
          |case when  b.flvurl is not  null  then b.flvurl
          |else a.content end as url
          |from mysql_result_temp  as a
          |left outer join
          |(select roomkey , flvurl  from minieastdaywap.dflive_room_base_info
          |where dt='${dt}' and appid='dftv' ) as b
          |on a.content =b.roomkey
          |
        """.stripMargin
      val dataset_mysql =spark.sql(sql_fetchId)
      dataset_mysql.registerTempTable("dataset_mysql")

      spark.sql("cache table dataset_mysql")
//      dataset_mysql.select($"modifydate" ,$"real_starttime", $"real_endtime",$"idx",$"pagenum",$"content",$"typename" )
//        .show(1000)
     // println(dataset_mysql.count())

//      val active_datasource =spark.sql(
//        s"""
//           |select clientime,dateline,urlfrom,idx02,pgnum,
//           |case when  regexp_extract(urlto,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2)=''
//           |then substring_index(urlto,'?',1)
//           |else regexp_extract(urlto,'(.*?)([a-zA-Z]{0,1}[0-9]{15,})(.html[\\?]{0,1}.*)',2) end  as content
//           |from minieastdaywap.log_miniwap_active_realtime
//           |where dt='${dt}' and apptypeid='DFTT'
//           |and substr(urlfrom,0,4) <> 'http' and substr(urlto,0,4) = 'http' and suptop ='0001'
//           |and clientime is not null and    clientime <> 'null' and clientime <> ''
//        """.stripMargin)
      val active_datasource =spark.sql(
        s"""
           |select clientime,dateline,urlfrom,idx02,pgnum,urlto
           |from minieastdaywap.log_miniwap_active_realtime
           |where dt='${dt}' and apptypeid='DFTT'and suptop ='0001'
        """.stripMargin)
      val active_datasourceRDD= active_datasource.rdd.filter(f=>{
        val urlfrom =f.getString(2)
        val urlto =f.getString(5)
        urlfrom !=null &&urlto!=null&&urlto.startsWith( "http")&& !urlfrom.startsWith( "http")

      }).filter(f=>{
        val clientime =f.getString(0)
        clientime!=null&& !clientime.equals("null") && !clientime.equals("")
      }).repartition(300).map(f=>{
          val  urlto =f.getString(5)
        var new_urlto=""
        val etl =ETLUtil.regexpExtract(urlto,"(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)",2)
        if(etl==null ){
          new_urlto=urlto.split("\\?")(0)
        }else{
          new_urlto=etl
        }
        ActiveLog(f.getString(0),f.getLong(1),f.getString(2),f.getString(3),f.getString(4),new_urlto)
      }).toDF("clientime","dateline","urlfrom","idx02","pgnum","content")
      active_datasourceRDD.registerTempTable("active_datasource")
      spark.sql("cache table active_datasource")
      //active_datasourceRDD.cache()
      active_datasourceRDD.show()
     // println(active_datasourceRDD.count())


      val sql_active =
        s"""
          |
          |select b.typename,b.idx,b.pagenum,b.content,b.real_starttime,b.real_endtime,a.clientime
          |from
          |(
          |select typename,idx,pagenum,
          | real_starttime, real_endtime ,content
          |from dataset_mysql
          |) as b
          |inner join
          |active_datasource as  a
          |on b.typename=a.urlfrom and b.idx=a.idx02 and b.pagenum=a.pgnum and b.content =a.content
          |and b.real_starttime <= a.dateline
          |and b.real_endtime >= a.dateline
          |where a.dateline is not null
          |
        """.stripMargin

      val dataset_active =spark.sql(sql_active)
      dataset_active.registerTempTable("active_pv_uv")
      //spark.sql("uncache table active_datasource")
     // println(dataset_active.count())
      import org.apache.spark.sql.functions._

      val aggUvPvActive =spark.sql(
        """
          |
          |select typename,idx,pagenum,content,real_starttime,real_endtime,
          | sum(active_sum_pv) as pv_active ,count(clientime) as uv_active
          |from(
          |select typename,idx,pagenum,content,clientime,real_starttime,real_endtime,count(clientime) as active_sum_pv
          |from active_pv_uv
          |group by typename,idx,pagenum,content,clientime,real_starttime,real_endtime
          |) as a
          |group by typename,idx,pagenum,content,real_starttime,real_endtime
          |
        """.stripMargin)
//      val aggUvPvActive =dataset_active.groupBy($"typename",$"idx",$"pagenum",$"content",$"clientime")
//        .agg(count("clientime") as "sum_pv"  )
//        .groupBy($"typename",$"idx",$"pagenum",$"content")
//        .agg(count("clientime") as "uv_active" ,sum("sum_pv") as "pv_active")
        aggUvPvActive.registerTempTable("agg_uv_pv_active")
        //aggUvPvActive.show(1000)
      val show_datasource=spark.sql(
        s"""
          |select ime,dateline,type ,idx,pagenum,
          |case when regexp_extract(url,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2)=''
          |then substring_index(url,'?',1)
          |else regexp_extract(url,'(.*?)([a-zA-Z]{0,1}[0-9]{1,})(.html[\\?]{0,1}.*)',2) end as content
          |from minieastdaywap.log_miniwap_appshow_realtime
          |where dt='${dt}' and domain='DFTT' and newstag ='0001'
          | and ime is not null and  ime <> 'null' and ime <> ''
        """.stripMargin)
      show_datasource.registerTempTable("show_datasource")


      val sql_show =
        s"""
           |
           |select b.typename,b.idx,b.pagenum,b.content,b.real_starttime,b.real_endtime,a.ime from
           |(
           |select typename,idx,pagenum,
           | real_starttime, real_endtime ,content
           |from dataset_mysql
           |) as b
           |inner  join
           |show_datasource as  a
           |on b.typename=a.type and b.idx=a.idx and b.pagenum=a.pagenum and b.content =a.content
           |and b.real_starttime <= a.dateline
           |and b.real_endtime >= a.dateline
           |where a.dateline is not null
           |
        """.stripMargin
      val dataset_show =spark.sql(sql_show)
      dataset_show.registerTempTable("show_pv_uv")

      val aggUvPvShow= spark.sql(
        """
          |select typename,idx,pagenum,content,real_starttime,real_endtime,
          | sum(show_sum_pv) as pv_show ,count(ime) as uv_show
          |from(
          |select typename,idx,pagenum,content,ime,real_starttime,real_endtime,count(ime) as show_sum_pv
          |from show_pv_uv
          |group by typename,idx,pagenum,content,ime,real_starttime,real_endtime
          |) as a
          |group by typename,idx,pagenum,content,real_starttime,real_endtime
          |
        """.stripMargin)


//      val aggUvPvShow= dataset_show
//        .groupBy($"typename",$"idx",$"pagenum",$"content",$"ime")
//        .agg( count("ime") as "show_sum_pv"  )
//        .groupBy($"typename",$"idx",$"pagenum",$"content")
//        .agg(count("ime") as "uv_show"  ,sum("show_sum_pv") as "pv_show")
        aggUvPvShow.registerTempTable("agg_uv_pv_show")
       // aggUvPvShow.show(1000)

      val sql_result =
        """
          |select a.modifydate,a.createuser,a.typename,a.newstype,a.title,a.idx,a.pagenum,
          | a.real_starttime, a.real_endtime, a.content, a.url ,b.uv_active,b.pv_active,c.uv_show,c.pv_show
          | from dataset_mysql  as a
          | left outer join
          | agg_uv_pv_active as b
          | on a.typename=b.typename and a.idx =b.idx and a.pagenum =b.pagenum  and a.content =b.content
          | and a.real_starttime=b.real_starttime and a.real_endtime=b.real_endtime
          | left outer join
          | agg_uv_pv_show as c
          | on a.typename=c.typename and a.idx =c.idx and a.pagenum =c.pagenum  and a.content =c.content
          | and a.real_starttime=c.real_starttime and a.real_endtime=c.real_endtime
        """.stripMargin

      val dataset_result =spark.sql(sql_result)
      dataset_result
        .select($"modifydate" ,$"real_starttime", $"real_endtime",$"idx"
        ,$"pagenum",$"content",$"typename",$"uv_active",$"pv_active",$"uv_show",$"pv_show" )
        .show(1000)
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

  def  spilt4NewTypes(data:Dataset[PagePosition] ): RDD[PagePosition] ={
    val aaa: RDD[PagePosition] = data.rdd.map(f=>(f.id,f)).groupByKey().flatMap{
      case (id ,iter) =>{
        val array = iter.toArray.sortBy(_.modifydate.getTime).reverse
        //判断开始时间
        def getRealStart(start_time_1:Timestamp,modify_1:Timestamp): Timestamp={
          if(start_time_1.getTime >= modify_1.getTime){
            start_time_1
          }else{
            modify_1
          }
        }
        //判断结束时间
        def getRealEnd(end_time_2:Timestamp,modify_2:Timestamp,modify_1:Timestamp): Timestamp ={
          if(end_time_2.getTime > modify_2.getTime && end_time_2.getTime>modify_1.getTime){
            modify_1
          }else{
            end_time_2
          }
        }
        if( array.length == 1){
          val array1 = ArrayBuffer[PagePosition]()
          val result =iter.map(aa=>{
            val format = new SimpleDateFormat("yyyyMMdd")
            aa.newstype.split(",").map(
              f=> {
                val bb =PagePosition(aa.id,aa.optype,aa.content,aa.title,aa.newstype,aa.idx,aa.pagenum,
                  aa.createuser,aa.modifydate,aa.starttime,aa.endtime,aa.sourcetype,aa.real_starttime,aa.real_endtime,
                  aa.typeid,aa.real_startdate,aa.real_enddate,aa.typename )
                bb.typeid=f
                bb.real_starttime=getRealStart(bb.starttime,bb.modifydate)
                bb.real_endtime=bb.endtime

                bb.real_startdate=format.format(new Date(bb.real_starttime.getTime))
                bb.real_enddate=format.format(new Date(bb.real_endtime.getTime))

                array1 += bb
              })
          })
          array1.toIterator

        }else{
          val array1 = ArrayBuffer[PagePosition]()
          val format = new SimpleDateFormat("yyyyMMdd")
          for (i <- 1 until  array.length){
            val first = array(i-1)
            val next = array(i)
            val start_time_1 =first.starttime
            val start_time_2 =next.starttime
            val end_time_1= first.endtime
            val end_time_2 =next.endtime
            val modify_1 =first.modifydate
            val modify_2 =next.modifydate
            //获取开始时间
            //val real_start_2 =getRealStart(start_time_2,modify_2)
            //next.real_starttime =real_start_2
            if(i==1){
              //first.real_starttime =getRealStart(start_time_1,modify_1)
              //first.real_endtime=first.endtime
              first.newstype.split(",").map(
                f=> {
                  val aa= first
                  val bb =PagePosition(aa.id,aa.optype,aa.content,aa.title,aa.newstype,aa.idx,aa.pagenum,
                    aa.createuser,aa.modifydate,aa.starttime,aa.endtime,aa.sourcetype,aa.real_starttime,aa.real_endtime,
                    aa.typeid,aa.real_startdate,aa.real_enddate,aa.typename )
                  bb.typeid=f

                  bb.real_starttime=getRealStart(bb.starttime,bb.modifydate)
                  bb.real_endtime=bb.endtime

                  bb.real_startdate=format.format(new Date(bb.real_starttime.getTime))
                  bb.real_enddate=format.format(new Date(bb.real_endtime.getTime))
                  array1 += bb
                })

            }
            next.real_endtime=getRealEnd(end_time_2,modify_2,modify_1)
            next.newstype.split(",").map(
              f=> {
                val aa= next
                val bb =PagePosition(aa.id,aa.optype,aa.content,aa.title,aa.newstype,aa.idx,aa.pagenum,
                  aa.createuser,aa.modifydate,aa.starttime,aa.endtime,aa.sourcetype,aa.real_starttime,aa.real_endtime,
                  aa.typeid,aa.real_startdate,aa.real_enddate,aa.typename )
                bb.typeid=f
                bb.real_starttime=getRealStart(bb.starttime,bb.modifydate)
                bb.real_startdate=format.format(new Date(bb.real_starttime.getTime))
                bb.real_enddate=format.format(new Date(bb.real_endtime.getTime))
//                bb.real_endtime=bb.endtime

                array1 += bb
              })
          }
          array1.toIterator

        }
      }
    }

    aaa
  }



}
