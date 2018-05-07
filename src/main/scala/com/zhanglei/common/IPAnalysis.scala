package com.zhanglei.common

import com.zhanglei.caseclass.{RegionInfo, IPRule}
import com.zhanglei.utils.Utils
import scala.util.control.Breaks._
/**
  * Created by Administrator on 2018/5/7.
  */
object IPAnalysis {

  def ip2Long(ip: String) = {
    val field = ip.split("[.]")
    var numIP:Long = 0L
    for(i <- 0 until (field.length)){
      numIP = numIP << 8 | field(i).toLong
    }
    numIP
  }

  def binerySearch(numIP: Long, ipRules: Array[IPRule]) = {
    var min:Int = 0
    var max:Int = ipRules.length - 1
    var index = -1
    breakable({
      while(min <= max){
        var middle = (min + max) / 2
        val ipRule = ipRules(middle)
        if(numIP >= ipRule.startIP && numIP <= ipRule.endIP){
          index = middle
          break()
        }else if(numIP < ipRule.startIP){
          max = middle - 1
        }else if(numIP > ipRule.endIP){
          min = middle + 1
        }
      }
    })
    index
  }

  def analysisIP(ip:String, ipRules:Array[IPRule]) = {
    val regionInfo = RegionInfo()
    if(Utils.validateIP(ip)){
      val numIP = ip2Long(ip)
      val index = binerySearch(numIP,ipRules)
      if(index != -1){
        val ipRule = ipRules(index)
        regionInfo.country = ipRule.country
        regionInfo.province = ipRule.province
        regionInfo.city = ipRule.city
      }
    }
    regionInfo
  }
}
