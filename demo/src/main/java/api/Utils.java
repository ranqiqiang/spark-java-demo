package api;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Created by qqr on 16/12/30.
 */
public class Utils {

    public static  SparkSession session = SparkSession.builder()
            .master("local[*]")
            .appName("demo")
            .getOrCreate();



    public static JavaSparkContext sc = new JavaSparkContext(session.sparkContext());


    public static Tuple2<Integer,Integer> newTuple2(Integer a,Integer b){
        return new Tuple2<Integer, Integer>(a,b);
    }
}
