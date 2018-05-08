package com.zhanglei.common

import com.alibaba.fastjson.JSON
import com.zhanglei.caseclass.IPRule
import com.zhanglei.constants.LogConstants
import com.zhanglei.utils.Utils
import org.apache.commons.lang.StringUtils
import scala.Predef
import scala.collection.mutable
/**
  * Created by Administrator on 2018/5/7.
  */
object LogAnalysis {

  def handlerIP(ip: String, ipRules: Array[IPRule], logMap: mutable.Map[String, String]) = {
    var regionInfo = IPAnalysis.analysisIP(ip,ipRules)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_COUNTRY,regionInfo.country)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_PROVINCE,regionInfo.province)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_CITY,regionInfo.city)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_IP,ip)
  }

  def handlerRequestParams(requestParams: String, logMap: mutable.Map[String, String]) = {
    val fields = requestParams.split("[?]")
    if(fields.length == 2){
      val dataArray = fields(1).split(" ")
      if(dataArray.length == 2){
        val data = dataArray(0).split("=")
        if(data.length == 2){
          val behavior = data(0)
          if(behavior.equals("bData")){
            logMap.put(LogConstants.LOG_COLUMNS_NAME_BEHAVIOR_FLAG,"bData")
          }else if(behavior.equals("pData")){
            logMap.put(LogConstants.LOG_COLUMNS_NAME_BEHAVIOR_FLAG,"pData")
          }
          val base64EncodeString = data(1)
          val jsonStr = new String(Utils.base64Decode(base64EncodeString))
          JSON.parseObject(jsonStr).entrySet().toArray().foreach(t => {
            val kv = t.toString.split("=")
            logMap.put(kv(0),kv(1))
          })
        }
      }
    }
  }
  //解析behaviorData
  def handlerBehaviorData(logMap: mutable.Map[String, String]) = {
    val behavior = logMap.getOrElse(LogConstants.LOG_COLUMNS_NAME_BEHAVIOR_DATA,null)
    if(behavior != null){
      val objectArray = JSON.parseObject(behavior)
      objectArray.entrySet().toArray().foreach(t => {
        val kv = t.toString.split("=")
        if(kv.length == 2){
          logMap.put(kv(0),kv(1))
        }
      })
    }
  }
  //设备信息
  def handlerDevice(deviceString: String, logMap: mutable.Map[String, String]) = {
    try{
      val fields = deviceString.split(";")
      if(fields.length > 2){
        val os = fields(2).trim().split(" ")
        if(os.length == 2){
          logMap.put(LogConstants.LOG_COLUMNS_NAME_OS_NAME,os(0))
          logMap.put(LogConstants.LOG_COLUMNS_NAME_OS_VERSION,os(1))
        }
        val modelNum = fields(3).split("[/]")(1).split("[)]")(0)
        logMap.put(LogConstants.LOG_COLUMNS_NAME_MODEL_NUM,modelNum)
      }
    }catch{
      case e:Exception => println(e.getMessage)
    }
  }

  def analysisLog(logText:String, ipRules:Array[IPRule]) = {
    var logMap:mutable.Map[String,String] = null
    try{
        if(StringUtils.isNotBlank(logText) && logText.contains("bData")){
          val fields = logText.split("[|]")
          if(fields.length == 8){
            logMap = mutable.Map[String,String]()
            handlerIP(fields(0),ipRules,logMap)
            logMap.put(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME,Utils.parseLogServerTimeToLong(fields(3)).toString)
            if(logText.contains("GET")){
              logMap.put(LogConstants.LOG_COLUMNS_NAME_REQUEST_TYPE,"GET")
            }else{
              logMap.put(LogConstants.LOG_COLUMNS_NAME_REQUEST_TYPE,"POST")
            }
            handlerRequestParams(fields(4),logMap)
            handlerBehaviorData(logMap)
            handlerDevice(fields(7),logMap)
          }
        }
    }catch {
      case e:Exception => println(e.getMessage)
    }
    logMap
  }
}
