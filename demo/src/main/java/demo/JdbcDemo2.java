package demo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import java.util.Properties;

/**
 * Created by qqr on 16/11/17.
 */
public class JdbcDemo2 {

    static String url="jdbc:mysql://10.1.6.10:3306/trade_conf?user=twodfire&password=123456";
    static String dbTable="kind_pay_detail";

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
                .load();

        dataset.createOrReplaceTempView("kind_pay_detail_option");
        dataset =  session.sql("select * from kind_pay_detail_option limit 10");


        dataset.createOrReplaceTempView("kind_pay_detail_option");


        JavaRDD javaRDD = dataset.toJavaRDD();
        System.out.println(javaRDD.count());
        dataset.persist(StorageLevel.MEMORY_ONLY());
//        dataset.unpersist();

        Dataset up_dataset = dataset.sparkSession().sql("select id,name from kind_pay_detail limit 100");
        JavaRDD up_javaRDD = up_dataset.toJavaRDD();

        System.out.println(up_javaRDD.count());

        Thread.sleep(10111100);
    }
}
