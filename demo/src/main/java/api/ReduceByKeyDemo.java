package api;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by qqr on 16/12/30.
 */
public class ReduceByKeyDemo extends BaseDemo {
    public static void main(String[] args) {
        JavaSparkContext sc = Utils.sc;

        List<Tuple2<Integer,Integer>> list = new ArrayList<Tuple2<Integer, Integer>>();

        list.add(newTuple2(1,1));
        list.add(newTuple2(1,1));
        list.add(newTuple2(1,3));
        list.add(newTuple2(1, 4));
        list.add(newTuple2(1, 5));
        list.add(newTuple2(2, 1));
        list.add(newTuple2(3, 1));
        list.add(newTuple2(4, 4));
        list.add(newTuple2(5, 4));
        list.add(newTuple2(6, 4));
        list.add(newTuple2(7, 4));

        JavaPairRDD<Integer,Integer> data = sc.parallelizePairs(list);

        // 按KEY 累加
        JavaPairRDD<Integer,Integer> pairRdd =  data.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v2+v2;
            }
        });
        pairRdd.foreachPartition(new VoidFunction<Iterator<Tuple2<Integer, Integer>>>() {
            public void call(Iterator<Tuple2<Integer, Integer>> it) throws Exception {
                while (it.hasNext()){
                    System.out.println(it.next());
                }
            }
        });

    }



}
