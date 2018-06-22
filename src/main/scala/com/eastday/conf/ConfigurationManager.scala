package com.eastday.conf

import java.io.InputStream
import java.util.Properties


/**
 * Created by admin on 2018/4/3.
 */
object ConfigurationManager {

  val prop =new Properties();
  val in :InputStream =ConfigurationManager.getClass.getClassLoader.getResourceAsStream("my.properties");
  prop.load(in)

  def getString(value:String): String ={
    prop.getProperty(value)
  }
  def getString(value:String,default:String): String ={
    prop.getProperty(value,default)
  }

  def getInteger(value:String) :Int={
    prop.getProperty(value).toInt
  }

  def getInteger(value:String,default:String) :Int={
    prop.getProperty(value,default).toInt
  }
  def getBoolean(value:String) :Boolean ={
    prop.getProperty(value).toBoolean
  }
  def getBoolean(value:String,default:String) :Boolean ={
    prop.getProperty(value,default).toBoolean
  }
  def getLong(value:String) :Long={
    prop.getProperty(value).toLong
  }
  def getLong(value:String,default:String) :Long={
    prop.getProperty(value,default).toLong
  }
}
