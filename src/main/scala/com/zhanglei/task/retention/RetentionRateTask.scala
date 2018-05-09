package com.zhanglei.task.retention

import com.zhanglei.constants.LogConstants
import com.zhanglei.utils.{JedisUtils, Utils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/9.
  */
object RetentionRateTask {

  def validateArgs(args: Array[String]) = {
    if(args.length < 2){
      println(
        s"""${this.getClass.getName}需要俩个参数
           |1,统计的日期:yyyy-MM-dd
           |2,统计周期:数字
      """.stripMargin)
      System.exit(0)
    }else if(!Utils.validateDate(args(0))){
      println(
        s"""
          |${this.getClass.getName}第一个参数应该是一个日期，格式为:yyyy-MM-dd
        """.stripMargin)
      System.exit(0)
    }else if(!Utils.validateNumber(args(1))){
      println(
        s"""
          |${this.getClass.getName}第二个参数应该是一个数字
        """.stripMargin)
      System.exit(0)
    }else{
      val runDate = Utils.parseDateToLong(args(0),"yyyy-MM-dd")
      val cycle = args(1).toInt
      //结束日期
      val nextDay = Utils.caculateDate(runDate,cycle)
      //当前日期
      val toDay = Utils.parseDateToLong(Utils.formatDate(System.currentTimeMillis(),"yyyy-MM-dd"),"yyyy-MM-dd")
      if(nextDay > toDay){
        println(
          """
            |所属的统计周期时间未到，还不能进行统计，程序退出
          """.stripMargin)
        System.exit(0)
      }
    }
  }

  def initScan(startTime: Long, endTime: Long) = {
    val scan = new Scan()
    //扫描开始位置
    scan.setStartRow(startTime.toString.getBytes())
    //扫描结束位置
    scan.setStopRow(endTime.toString.getBytes())
    //指定扫描的log列族的deviceId这一列
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes())
    scan
  }

  def convertScanToBase64Scan(scan: Scan) = {
    val protoScan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(protoScan.toByteArray)
  }

  def loadDataFromHbase(startTime: Long, endTime: Long, sparkSession: SparkSession) = {
    println(s"正在加载 ${Utils.formatDate(startTime, "yyyy-MM-dd HH:mm:ss")} 到  ${Utils.formatDate(endTime, "yyyy-MM-dd HH:mm:ss")} 的数据")
    //初始化一个scan
    val scan = initScan(startTime,endTime)
    val base64Scan = convertScanToBase64Scan(scan)
    //创建一个配置文件对象
    val jobConf = new JobConf(new Configuration())
    jobConf.set(TableInputFormat.SCAN,base64Scan)
    jobConf.set(TableInputFormat.INPUT_TABLE,LogConstants.LOG_HBASE_TABLE)

    val userRdd = sparkSession.sparkContext.newAPIHadoopRDD(jobConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable]
      ,classOf[Result]).map(t2 => {
        val result = t2._2
      var deviceId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes()))
      (deviceId,deviceId)
    })
    userRdd
  }

  def main(args: Array[String]) {
    //验证参数
    validateArgs(args)
    //取出参数
    val Array(runDate,cycle) = args

    val sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()

    //基准日开始的时间和结束时间
    var startTime = Utils.parseDateToLong(runDate,"yyyy-MM-dd")
    var endTime = Utils.caculateDate(startTime,1)
    //加载数据
    val todayUserRdd = loadDataFromHbase(startTime,endTime,sparkSession).distinct()

    //统计周期之后的日期开始时间和结束时间
    startTime = Utils.caculateDate(startTime,cycle.toInt)
    endTime = Utils.caculateDate(startTime,1)
    val nextDayUserRdd = loadDataFromHbase(startTime,endTime,sparkSession).distinct()

    //统计当天的用户数
    val todayUserCount = todayUserRdd.count()
    //统计留存周期后那一天的用户数
    val nextDayUserCount = todayUserRdd.join(nextDayUserRdd).count()
    if(todayUserCount != 0){
      val value = Utils.getScale(nextDayUserCount.toDouble / todayUserCount,2)
      val key = s"retention_rate_${cycle}_day"
      val field = runDate
      //将结果写入redis
      JedisUtils.hset(key,field,value.toString())
    }else{
      println(
        """
          |统计当天新增用户为零
        """.stripMargin)
    }
    sparkSession.close()
  }
}
