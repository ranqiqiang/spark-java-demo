package api;

import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * @author <a href="mailto:huoguo@2dfire.com">火锅</a>
 * @time 17/5/26
 */
public class BaseDemo {

    static  JavaSparkContext sc = Utils.sc;

    static Tuple2<Integer,Integer> newTuple2(Integer a, Integer b){
        return Utils.newTuple2(a,b);
    }
}
