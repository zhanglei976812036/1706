package com.zhanglei.constants

/**
  * Created by Administrator on 2018/5/7.
  */
object LogConstants {
  final val LOG_COLUMNS_NAME_IP = "ip"
  final val LOG_COLUMNS_NAME_ACCESS_TIME = "accessTime"
  final val LOG_COLUMNS_NAME_COUNTRY = "country"
  final val LOG_COLUMNS_NAME_PROVINCE = "province"
  final val LOG_COLUMNS_NAME_CITY = "city"
  final val LOG_COLUMNS_NAME_REQUEST_TYPE = "requestType"
  //用户行为标识
  final val LOG_COLUMNS_NAME_BEHAVIOR_FLAG = "behaviorFlag"
  //用户行为数据
  final val LOG_COLUMNS_NAME_BEHAVIOR_DATA = "behaviorData"
  final val LOG_COLUMNS_NAME_OS_NAME = "osName"
  final val LOG_COLUMNS_NAME_OS_VERSION = "osVersion"
  //设备型号
  final val LOG_COLUMNS_NAME_MODEL_NUM = "modeNun"
  final val LOG_HBASE_TABLE:String = "kugou_music_log"
  final val LOG_HBASE_TABLE_FAMILY:String = "log"
}
