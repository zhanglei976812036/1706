package com.zhanglei.common

import com.alibaba.fastjson.JSON
import com.zhanglei.caseclass.IPRule
import com.zhanglei.constants.LogConstants
import com.zhanglei.utils.Utils
import org.apache.commons.lang.StringUtils
import scala.collection.mutable
/**
  * Created by Administrator on 2018/5/7.
  */
object LogAnalysis {

  def handleIP(ip: String, ipRules: Array[IPRule], logMap: mutable.Map[String, String]) = {
    val regionInfo = IPAnalysis.analysisIP(ip,ipRules)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_COUNTRY,regionInfo.country)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_PROVINCE,regionInfo.province)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_CITY,regionInfo.city)
    logMap.put(LogConstants.LOG_COLUMNS_NAME_IP,ip)
  }

  def handleRequestParams(requestParams: String, logMap: mutable.Map[String, String]) = {
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
          val jsonStr = new Predef.String(Utils.base64Decode(base64EncodeString))
          JSON.parseObject(jsonStr).entrySet().toArray().foreach(t => {
            val kv = t.toString.split("=")
            logMap.put(kv(0),kv(1))
          })
         }
      }
    }
  }
  //处理behaviorData数据
  def handleBehaviorData(logMap: mutable.Map[String, String]) = {
    val behaviorData = logMap.getOrElse(LogConstants.LOG_COLUMNS_NAME_BEHAVIOR_DATA,null)
    if(behaviorData != null){
      val objectArray = JSON.parseObject(behaviorData)
      objectArray.entrySet().toArray().foreach(t => {
        val kv = t.toString.split("=")
        if(kv.length == 2){
          logMap.put(kv(0),kv(1))
        }
      })
    }
  }
  //处理设备信息
  def handleDevice(deviceString: String, logMap: mutable.Map[String, String]) = {
      try{
        val fields = deviceString.split(";")
        if(fields.length > 2){
          //把俩边的空格切掉
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

  def analysisLog(longText:String, ipRules:Array[IPRule]) = {
    var logMap: mutable.Map[String, String] = null
    try{
      if(StringUtils.isNotBlank(longText)){
        val fields = longText.split("[|]")
        if(fields.length == 8){
          logMap = mutable.Map[String,String]()
          //解析ip
          handleIP(fields(0),ipRules,logMap)
          //处理请求时间
          logMap.put(LogConstants.LOG_COLUMNS_NAME_ACCESS_TIME,Utils.parseLogServerTimeToLong(fields(3)).toString)
          //处理请求方式
          if(longText.contains("GET")){
            logMap.put(LogConstants.LOG_COLUMNS_NAME_REQUEST_TYPE,"GET")
          }else{
            logMap.put(LogConstants.LOG_COLUMNS_NAME_REQUEST_TYPE,"POST")
          }
          //处理请求数据
          handleRequestParams(fields(4),logMap)
          //处理behaviorData数据
          handleBehaviorData(logMap)
          //处理设备信息
          handleDevice(fields(7),logMap)
        }
      }
    }catch {
      case e:Exception => println(e.getMessage)
    }
    logMap
  }
}
