package com.zhanglei.tags

import com.zhanglei.bean.Logs
import com.zhanglei.constants.TagsConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Created by Administrator on 2018/5/14.
  */
object Tags4Area extends Tags{
  //打标签的方法
  override def makeTags(args: Any*) = {
    val areaMap = mutable.Map[String,Int]()
    if(args.length > 0){
      val logs = args(0).asInstanceOf[Logs]
      if(StringUtils.isNotBlank(logs.country)){
        areaMap += (TagsConstants.AREA+logs.country+"_"+logs.province+"_"+logs.city -> 1)
      }
    }
    areaMap
  }
}
