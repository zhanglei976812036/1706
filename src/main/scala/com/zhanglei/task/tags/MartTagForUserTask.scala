package com.zhanglei.task.tags

import com.zhanglei.bean.Logs
import com.zhanglei.constants.LogConstants
import com.zhanglei.tags.Tags4UserInterested
import com.zhanglei.utils.Utils
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{Bytes, Base64}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Administrator on 2018/5/14.
  */
object MartTagForUserTask {
  //验证参数
  def validateArgs(args: Array[String]) = {
    if(args.length < 1 && Utils.validateDate(args(0))){
      println(s"${this.getClass.getName}需要一个：yyyy-MM-dd 格式的日期参数")
      System.exit(0)
    }
  }
  //初始化扫描器
  def initScan(startTime:Long,endTime:Long) = {
    val scan = new Scan()
    scan.setStartRow(startTime.toString.getBytes())
    scan.setStopRow(endTime.toString.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_ALBUM_ID.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_ANCHOR_ID.getBytes())
    scan.addColumn(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_PLAY_TIME.getBytes())
    scan
  }
  //普通Scan转换成base64字符串
  def convertBase64Scan(scan:Scan) = {
    val protoScan = ProtobufUtil.toScan(scan)
    Base64.encodeBytes(protoScan.toByteArray)
  }
  //从hbase中加载数据
  def loadLogDateFromHbase(startTime:Long,endTime:Long,sparkSession:SparkSession) = {
    val scan = initScan(startTime,endTime)
    val base64Scan = convertBase64Scan(scan)
    val jobConf = new JobConf(base64Scan)
    jobConf.set(TableInputFormat.INPUT_TABLE,LogConstants.LOG_HBASE_TABLE)
    jobConf.set(TableInputFormat.SCAN,base64Scan)

    val tags4LogRDD = sparkSession.sparkContext.newAPIHadoopRDD(jobConf,classOf[TableInputFormat],classOf[ImmutableBytesWritable],
      classOf[Result]).map(t2 =>{
      val result = t2._2
      val deviceId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_DEVICE_ID.getBytes()))
      val albumId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_ALBUM_ID.getBytes()))
      val anchorId = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_ANCHOR_ID.getBytes()))
      val playTime = Bytes.toString(result.getValue(LogConstants.LOG_HBASE_TABLE_FAMILY.getBytes(),LogConstants.LOG_COLUMNS_NAME_PLAY_TIME.getBytes()))
      val logs = new Logs(deviceId = deviceId,albumId = albumId,anchorId = anchorId,playTime = playTime)
      (deviceId,Tags4UserInterested.makeTags(logs).toList)
    }).filter(t2 => t2._2.size >0)
    tags4LogRDD
  }
  def main(args: Array[String]) {
    //验证日期
    validateArgs(args)

    val startTime = Utils.parseDateToLong(args(0),"yyyy-MM-dd")
    val endTime = Utils.caculateDate(startTime,7)

    val sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName).master("local").getOrCreate()

    val tags4LogRDD = loadLogDateFromHbase(startTime,endTime,sparkSession)

    tags4LogRDD.reduceByKey{
      case (list0,list1) => {
      (list0 ++ list1).groupBy(x => x._1).map(g => (g._1,g._2.map(x => x._2).sum)).toList
        }
    }
    sparkSession.stop()
    }
}
