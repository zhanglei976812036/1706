package com.zhanglei.bean

/**
  * Created by Administrator on 2018/5/14.
  */
class Logs (
           var deviceId:String = null,//设备id
           var ip:String = null,//ip
           var country:String = null,//国家
           var province:String = null,//省份
           var city:String = null,//城市
           var albumId:String = null,//专辑id
           var programId:String = null,//节目id
           var anchorId:String = null,//主播id
           var zongKey:String = null,//app区域信息
           var playTime:String = null,//播放时长
           var accessTime:String = null,//访问时间
           var behaviorKey:String = null,//行为标识key
           var behaviorData:String = null,//用户行为数据
           var modelNum:String = null,//手机型号
           var requestType:String = null,//请求方式
           var osName:String = null,//操作系统名称
           var osVersion:String = null//操作系统版本
           ){

}
