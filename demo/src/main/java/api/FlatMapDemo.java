package api;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="mailto:huoguo@2dfire.com">火锅</a>
 * @time 17/5/26
 */
public class FlatMapDemo extends BaseDemo {
    public static void main(String[] args) {


        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();

        list.add(newTuple2(2,11));
        list.add(newTuple2(3,22));
        list.add(newTuple2(4,33));
        list.add(newTuple2(5, 44));

        JavaPairRDD<Integer,Integer> data = sc.parallelizePairs(list);
        final JavaRDD<Integer> map = data.flatMap(new FlatMapFunction<Tuple2<Integer,Integer>, Integer>() {

            public Iterator<Integer> call(Tuple2<Integer, Integer> it) throws Exception {
                // 直接返回集合，可以生成多了 对象，比如1，2，3，4，5 一起返回
                return Arrays.asList(it._1).iterator();
            }
        });

        map.foreach(new VoidFunction<Integer>() {
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

       List<Integer>  result =  map.collect();
       for(Integer v : result){
           System.out.println(v);
       }
    }



}
