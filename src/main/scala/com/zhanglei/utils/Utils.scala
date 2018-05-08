package com.zhanglei.utils

import java.text.SimpleDateFormat
import java.util.{Base64, Locale}
import java.util.regex.Pattern

/**
  * Created by Administrator on 2018/5/7.
  */
object Utils {
  def validate(ip:String) = {
    val reg = "((25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))\\.){3}(25[0-5]|2[0-4]\\d|((1\\d{2})|([1-9]?\\d)))"
    val pattern = Pattern.compile(reg)
    pattern.matcher(ip).matches()
  }
  def parseLogServerTimeToLong(accessTime:String) = {
    val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss +8000",Locale.ENGLISH)
    val date = simpleDateFormat.parse(accessTime)
    date.getTime
  }
  def base64Decode(base64EncodeString:String) = {
    Base64.getDecoder.decode(base64EncodeString)
  }
  def validateDate(date: String) = {
    val reg = "((((19|20)\\d{2})-(0?(1|[3-9])|1[012])-(0?[1-9]|[12]\\d|30))|(((19|20)\\d{2})-(0?[13578]|1[02])-31)|(((19|20)\\d{2})-0?2-(0?[1-9]|1\\d|2[0-8]))|((((19|20)([13579][26]|[2468][048]|0[48]))|(2000))-0?2-29))$"
    val pattern = Pattern.compile(reg)
    pattern.matcher(date).matches()
  }
  /**
    * 将时间戳转换成指定格式的日期
    *
    * @param longTime
    * @param pattern
    * @return
    */
  def formatDate(longTime: Long, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    val calendar = sdf.getCalendar
    calendar.setTimeInMillis(longTime)
    sdf.format(calendar.getTime)
  }
  /**
    * 将字符串时间转换成 long类型的时间戳
    *
    * @param strDate 2017-10-23
    * @param pattern yyyy-MM-dd
    */
  def parseDateToLong(strDate: String, pattern: String) = {
    val sdf = new SimpleDateFormat(pattern)
    sdf.parse(strDate).getTime
  }

}
