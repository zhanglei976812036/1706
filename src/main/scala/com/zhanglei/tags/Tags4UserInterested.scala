package com.zhanglei.tags

import com.zhanglei.bean.Logs
import com.zhanglei.constants.TagsConstants
import org.apache.commons.lang.StringUtils

import scala.collection.mutable

/**
  * Created by Administrator on 2018/5/14.
  */
object Tags4UserInterested extends Tags{
  //打标签的方法
  override def makeTags(args: Any*): mutable.Map[String, Int] = {
    val userInterestedMap = mutable.Map[String,Int]()
    if(args.length > 0 ){
      val logs = args(0).asInstanceOf[Logs]
      if(StringUtils.isNotBlank(logs.albumId))
        userInterestedMap += (TagsConstants.INTERESTED_ALBUM_ID_PREFIX + logs.albumId -> 1)
      if(StringUtils.isNotBlank(logs.anchorId))
        userInterestedMap += (TagsConstants.INTERESTED_ANCHOR_ID_PREFIX + logs.anchorId -> 1)
      if(StringUtils.isNotBlank(logs.playTime)){
        val playTimeLong = logs.playTime.toInt / 60
        if(playTimeLong > 0 && playTimeLong <=5){
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "0_5" -> 1)
        } else if (playTimeLong > 10 && playTimeLong <= 20) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "10_20" -> 1)
        } else if (playTimeLong > 20 && playTimeLong <= 30) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "20_30" -> 1)
        } else if (playTimeLong > 30) {
          userInterestedMap += (TagsConstants.INTERESTED_PROGRAM_TIME_LEVEL_PREFIX + "30_plus" -> 1)
        }
      }
    }
    userInterestedMap
  }
}
