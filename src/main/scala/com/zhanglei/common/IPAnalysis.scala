package com.zhanglei.common

import com.zhanglei.caseclass.{RegionInfo, IPRule}
import com.zhanglei.utils.Utils
import scala.util.control.Breaks._
/**
  * Created by Administrator on 2018/5/7.
  */
object IPAnalysis {

  def ip2Long(ip: String) = {
    val fields = ip.split("[.]")
    var numIP:Long = 0L
    for(i <- 0 until (fields.length)){
      numIP = numIP << 8 | fields(i).toLong
    }
    numIP
  }

  def binnerySearch(numIP: Long, ipRules: Array[IPRule]) = {
    var min = 0
    var max = ipRules.length - 1
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
        }else if(numIP < ipRule.endIP){
          min = middle + 1
        }
      }
    })
    index
  }

  def analysisIP(ip:String, ipRules:Array[IPRule]) = {
    val regionInfo = RegionInfo()
    if(Utils.validate(ip)){
      val numIP = ip2Long(ip)
      var index = binnerySearch(numIP,ipRules)
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
