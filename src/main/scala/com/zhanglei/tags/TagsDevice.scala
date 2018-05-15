package com.zhanglei.tags

import com.zhanglei.bean.Logs
import com.zhanglei.constants.TagsConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Created by Administrator on 2018/5/14.
  */
object TagsDevice extends Tags{
  //打标签的方法
  override def makeTags(args: Any*): mutable.Map[String, Int] = {
    val deviceMap = mutable.Map[String,Int]()
    if(args.length > 0){
      val logs = args(0).asInstanceOf[Logs]
      if(StringUtils.isNotBlank(logs.deviceId))
        deviceMap += (TagsConstants.DEVICE + logs.deviceId -> 1)
      if(StringUtils.isNotBlank(logs.osName))
        deviceMap += (TagsConstants.DEVICE + logs.osName -> 1)
      if(StringUtils.isNotBlank(logs.modelNum))
        deviceMap += (TagsConstants.DEVICE + logs.modelNum -> 1)
    }
    deviceMap
  }
}
