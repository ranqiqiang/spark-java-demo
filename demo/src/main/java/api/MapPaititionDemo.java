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
public class MapPaititionDemo extends BaseDemo {
    public static void main(String[] args) {


        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();

        list.add(newTuple2(2,11));
        list.add(newTuple2(3,22));
        list.add(newTuple2(4,33));
        list.add(newTuple2(2, 44));

        JavaPairRDD<Integer,Integer> data = sc.parallelizePairs(list);
        final JavaRDD<Object> map = data.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer,Integer>>, Object>() {

            // 多分区 做map 操作，大数据量 可以分多个pt操作 再聚合，并发操作快
            public Iterator<Object> call(Iterator<Tuple2<Integer, Integer>> it) throws Exception {
                List l = new ArrayList();
                while (it.hasNext()){
                    l.add(it.next()._1);
                }
                return  l.iterator();
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
