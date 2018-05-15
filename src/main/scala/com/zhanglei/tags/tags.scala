package com.zhanglei.tags

import scala.collection.mutable
/**
  * Created by Administrator on 2018/5/14.
  */
trait Tags {
  //打标签的方法
  def makeTags(args:Any*):mutable.Map[String,Int]

}
