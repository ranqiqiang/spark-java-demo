package demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.collection.Seq;
import scala.collection.mutable.*;
import scala.collection.mutable.StringBuilder;

import java.util.Properties;
import java.util.List;

/**
 * Created by qqr on 16/11/17.
 */
public class JdbcDemo {

    static String url="jdbc:mysql://10.1.6.104:3306/order127?user=order&password=order@552208";
    static String dbTable="orderdetail";

    static Properties properties = new Properties();
    static {
        properties.put("fetchSize",10000);
    }

    public static void main(String[] args) throws InterruptedException {
        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("jdbcDemo")
                .getOrCreate();
        Dataset dataset =  session.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", dbTable)
//                .option("partitionColumn", "load_time")
//                .option("lowerBound", 946684929).option("upperBound", 1479374300)
//                .option("numPartitions", 1)
                .load();

        dataset.createOrReplaceTempView("test");


        dataset = session.sql("select op_time from test ");
        dataset.persist(StorageLevel.MEMORY_ONLY());

        long a = System.currentTimeMillis();
        System.out.println(dataset.count());
        long b = System.currentTimeMillis();
        System.out.println(b-a);


        Thread.sleep(111111111);
    }
}
