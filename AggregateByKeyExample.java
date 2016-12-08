import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;

import scala.Tuple2;

public class AggregateByKeyExample {

	@SuppressWarnings({ "unchecked", "serial" })
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("aggByKeyExpriment");
		String[] keyValArray = {"one=a", "one=b", "one=a", "one=c",  "one=akakakakaka", "one=d", "a=d"};
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> data = sc.parallelize(Arrays.asList(keyValArray));

		//map array to pairs
		JavaPairRDD<String, String> pairs = data.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, String>() {

			@Override
			public Iterator<Tuple2<String, String>> call(Iterator<String> arg0)
					throws Exception {
				ArrayList<Tuple2<String, String>> arraylist = new ArrayList<>();
				while(arg0.hasNext()) {
					String kvp[] = arg0.next().split("=");
					arraylist.add(new Tuple2<String, String>(kvp[0], kvp[1]));
				}
				return arraylist.iterator();
			}
		});

		System.out.println(pairs.take(10));

		//concat values by key [ K=Str, V=str , U = list<str> ]
		//zero value
		ArrayList<String> zeroValue = new ArrayList<String>();

		//merge V's into U's
		Function2 transformFunc = new Function2<ArrayList<String>, //U (what we want to return)
				String, //V (like in our pairs)
				ArrayList<String>>() { //U - i got no idea why i NEED to specify this again...

			@Override
			public ArrayList<String> call(
					ArrayList<String> key,
					String value) throws Exception {
				ArrayList<String> al = new ArrayList<String>();
				al.add(value);
				return al;
			}
		};

		//merge U's together
		Function2 combFunc = new Function2<ArrayList<String>,
				ArrayList<String>,
				ArrayList<String>>() {

			@Override
			public ArrayList<String> call(
					ArrayList<String> arg0,
					ArrayList<String> arg1) throws Exception {
				arg0.addAll(arg1);
				return arg0;
			}
		};

		JavaPairRDD aggregateByKey = pairs.aggregateByKey(zeroValue, transformFunc, combFunc).cache();
		System.out.println(aggregateByKey.collect());

	}

}
