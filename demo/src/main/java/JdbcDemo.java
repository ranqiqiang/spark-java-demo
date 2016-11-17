import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

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

    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("jdbcDemo")
                .getOrCreate();
        Dataset dataset =  session.read()
                .format("jdbc")
                .option("url",url)
                .option("dbtable", dbTable)
                .option("partitionColumn", "load_time")
                .option("lowerBound", 946684929).option("upperBound", 1479374300)
                .option("numPartitions",10)
                .load();
        long count2 =  dataset.count();
        System.out.println(count2);
    }
}
