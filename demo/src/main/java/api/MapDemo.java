package api;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:huoguo@2dfire.com">火锅</a>
 * @time 17/5/26
 */
public class MapDemo extends BaseDemo {
    public static void main(String[] args) {
        JavaSparkContext sc = Utils.sc;

        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();

        list.add(newTuple2(2,11));
        list.add(newTuple2(3,22));
        list.add(newTuple2(4,33));
        list.add(newTuple2(5, 44));

        JavaPairRDD<Integer,Integer> data = sc.parallelizePairs(list);
        final JavaRDD<Object> map = data.map(new Function<Tuple2<Integer, Integer>, Object>() {
            public Object call(Tuple2<Integer, Integer> v1) throws Exception {
                // K V 键值对
                return v1._1;
            }
        });

        map.foreach(new VoidFunction<Object>() {
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });

        List<Object>  result =  map.collect();
        for(Object v : result){
            System.out.println(v);
        }
    }



}
