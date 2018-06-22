package com.eastday.domain

import java.sql.Timestamp


/**
 * Created by admin on 2018/6/8.
 */


/**
 *
 * @param id
 * @param optype
 * @param content
 * @param title
 * @param newstype
 * @param idx
 * @param pagenum
 * @param createuser
 * @param modifydate
 * @param starttime
 * @param endtime
 * @param sourcetype
 * @param real_starttime
 * @param real_endtime
 * @param typeid
 * @param real_startdate
 * @param real_enddate
 * @param typename
 */
  case class PagePosition(id:Int, optype:String,content:String,title:String,newstype:String,idx:String,pagenum:String,
                          createuser:String,modifydate:Timestamp, starttime:Timestamp,endtime:Timestamp,sourcetype:String,

                          var real_starttime:Timestamp, var real_endtime:Timestamp, var typeid:String,var  real_startdate:String,var real_enddate :String ,typename:String )

case class ActiveLog(clientime:String,dateline:Long,urlfrom:String,idx02:String,pgnum:String,urlto:String)
case class AppFlowDomain(dt:String,hour :String, title:String ,type1:String ,url:String ,rate :Double)