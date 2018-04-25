import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkContext._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SQLContext
import scala.xml.XML
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Calendar
import java.sql.DriverManager
import java.util.Properties
import java.io.FileInputStream
import scala.util.{Success, Try}

object Hbase2Hive {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: cn.ac.iie.iaa.Hbase2Hive <hbase2hive_profile> last_date")
      System.exit(1)
    }

    val xml = XML.loadFile(args(0))
    
    var provLongID = (xml \\ "province_long_id").text

    var yesterday = args(1).toString()

    val splitPattern = (xml \\ "src_split_pattern").text
    
    val hiveDb = (xml \\ "hive_db").text
    val hiveTb = (xml \\ "hive_tb").text
    
    val hbaseRootDir = (xml \\ "hbase_root_dir").text
    val hbaseZkPort = (xml \\ "hbase_zookeeper_port").text
    val habseZkQuorum = (xml \\ "hbase_zookeeper_quorum").text //hbase 配置    
    val hbaseTable = (xml \\ "hbase_table").text
    val hbaseTableCf = (xml \\ "hbase_table_cf").text
    val hbaseTableCount = (xml \\ "hbase_table_col1").text
    val hbaseTableDay = (xml \\ "hbase_table_col2").text
    
    val sparkConf = new SparkConf().setAppName("Hbase2Hive")
    val sc = new SparkContext(sparkConf)
    val hict = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)
    import hict.implicits._

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.property.clientPort", hbaseZkPort) //集群zookeeper端口
    conf.set("hbase.zookeeper.quorum", habseZkQuorum) //zookeeper节点
    conf.set(TableInputFormat.INPUT_TABLE, hbaseTable) //设置查询的表名    

    val dateFormat = new SimpleDateFormat("yyyyMMdd")
    var cal: Calendar = Calendar.getInstance()
    cal.add(Calendar.DATE, -1)
    // var yesterday = dateFormat.format(cal.getTime())
    
    val hbaserdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])
    hbaserdd.cache()

    //读取HBASE表取出key，并去除key中的分钟标志
    val rdd = hbaserdd.map(line => (Bytes.toString(line._2.getRow).substring(12),
      Bytes.toString(line._2.getValue(hbaseTableCf.getBytes(), hbaseTableDay.getBytes())),
      Bytes.toString(line._2.getValue(hbaseTableCf.getBytes(), hbaseTableCount.getBytes())).toLong))// .filter(temp => temp._2 == yesterday) //过滤出前一天日志

    val tmp = rdd.filter(temp => temp._2 == yesterday)

    val Group = tmp.map(line => (line._1, line._3)).reduceByKey(_ + _)

    val splitKeyGroup = Group.map{ case(key, value) =>
       val splitKey = key.split("\\|")
       val toIntFlag = scala.util.Try(splitKey(3).toInt) match {       
          case Success(_) => "Int";
          case _ =>  "notInt";
        }
       val toLongFlag = scala.util.Try(value.toLong) match {       
          case Success(_) => "Long";
          case _ =>  "noLong";
        }
       if(splitKey.length == 7 && toIntFlag == "Int" && toLongFlag == "Long"){
         (splitKey(0), splitKey(1), splitKey(2), splitKey(3).toInt, splitKey(4), splitKey(5), splitKey(6), value.toLong)
       }else{
         ("NULL", "NULL", "NULL", 0.toInt, "NULL", "NULL", "NULL", 0.toLong)
       }
    } 

    val rddResult = splitKeyGroup.toDF("pp_0101", "dd_0001", "dd_0005", "dd_0004", "ip_0001", "uu_type", "op_0201x", "rr_x1112")

    rddResult.registerTempTable("dnslog_temp")

    val createTable = "create table if not exists TB_R_W_DNS_LOG_PARSE_C(pp_0101 string, dd_0001 string, dd_0005 string, dd_0004 int, ip_0001 string, uu_type string, op_0201x string, rr_x1112 bigint) partitioned by(ds string, prov string) row format delimited fields terminated by '\u0001'"
    hict.sql("use " + hiveDb) //数据库
    hict.sql(createTable)
    hict.sql("insert into " + hiveTb + " partition(ds='" + yesterday + "', prov='" + provLongID + "') select * from dnslog_temp")

    sc.stop()
  }
}