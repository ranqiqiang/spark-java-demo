package api;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author <a href="mailto:huoguo@2dfire.com">火锅</a>
 * @time 17/5/26
 */
public class MapPaititionWithIndexDemo extends BaseDemo {
    public static void main(String[] args) {


        List<Tuple2<Integer, Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();

        list.add(newTuple2(2, 11));
        list.add(newTuple2(3, 22));
        list.add(newTuple2(4, 33));
        list.add(newTuple2(2, 44));

        JavaPairRDD<Integer, Integer> data = sc.parallelizePairs(list);
        // 默认8个分区，从新分区
        data = data.repartition(4);

        final JavaRDD<Object> map = data.mapPartitionsWithIndex(new Function2<Integer, Iterator<Tuple2<Integer, Integer>>, Iterator<Object>>() {
            public Iterator<Object> call(Integer v1, Iterator<Tuple2<Integer, Integer>> it) throws Exception {

                System.out.println("分区索引:"+v1);
                List l = new ArrayList();
                while (it.hasNext()) {
                    l.add(it.next()._1);
                }
                return l.iterator();
            }
            // preservesPartitioning表示是否保留父RDD的partitioner分区信息。
        }, false);

        map.foreach(new VoidFunction<Object>() {
            public void call(Object o) throws Exception {
                System.out.println(o);
            }
        });


    }


}
