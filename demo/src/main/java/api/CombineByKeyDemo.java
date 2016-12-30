package api;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by qqr on 16/12/30.
 */
public class CombineByKeyDemo {
    public static void main(String[] args) {
        SparkSession session = SparkSession.builder()
                .master("local[*]")
                .appName("jdbcDemo")
                .getOrCreate();
        JavaSparkContext sc = new JavaSparkContext(session.sparkContext());


        List<Tuple2<Integer,Integer>>  list = new ArrayList<Tuple2<Integer, Integer>>();

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

        JavaPairRDD<Integer,List<Integer>> result = data.combineByKey(
                // 新值 直接添加
                new Function<Integer, List<Integer>>() {
                    public List<Integer> call(Integer v1) throws Exception {
                        List<Integer> values = new ArrayList<Integer>();
                        values.add(v1);
                        return values;
                    }
                },
                // 存在 直接添加
                new Function2<List<Integer>, Integer, List<Integer>>() {
                    public List<Integer> call(List<Integer> v1, Integer v2) throws Exception {
                        v1.add(v2);
                        return v1;
                    }
                },
                //
                new Function2<List<Integer>, List<Integer>, List<Integer>>() {
                    public List<Integer> call(List<Integer> v1, List<Integer> v2) throws Exception {
                        v1.addAll(v2);
                        return v1;
                    }
                }
        );

        result.foreach(new VoidFunction<Tuple2<Integer, List<Integer>>>() {
            public void call(Tuple2<Integer, List<Integer>> it) throws Exception {
                System.out.println(it._1()+":"+ Arrays.toString(it._2().toArray()));
            }
        });

    }

    private static Tuple2<Integer,Integer> newTuple2(Integer a,Integer b){
        return new Tuple2<Integer, Integer>(a,b);
    }
}
