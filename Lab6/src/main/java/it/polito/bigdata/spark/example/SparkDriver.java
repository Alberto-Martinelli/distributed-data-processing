package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;

import scala.Tuple2;
import java.util.List;
import java.util.ArrayList;
import org.apache.spark.SparkConf;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		String inputPath;
		String outputPath;
		
		inputPath=args[0];
		outputPath=args[1];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #6");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #6").setMaster("local");
		// Remember to remove .setMaster("local") before running your application on the cluster
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");
		
		// Read the content of the input file
		JavaRDD<String> reviewsRDD = sc.textFile(inputPath);

		// user --> list of (distinct) products reviewed
		JavaPairRDD<String, Iterable<String>> userToDuplicatedProducts =
					 reviewsRDD.filter(line -> !line.startsWith("Id,")) //remove first row
					//map to PairRDD
					.mapToPair((line)->{
						String user_id = line.split(",")[2];
						String product_id = line.split(",")[1];
						return new Tuple2<String, String>(user_id, product_id);
					})
					.groupByKey()
					//remove duplicates
					.mapValues((iterable)->{
						List<String> myList = new ArrayList<>();
						for(String s : iterable){
							if(!myList.contains(s))
								myList.add(s);
						}
						return myList;
					});
			
		//2) Count the frequency of each pair of products that have been reviewed together 
		JavaPairRDD<Integer, String> frequencyOfCouples =
								 userToDuplicatedProducts.values()
								//(product1, product2)--># of users that reviewed both
								.flatMapToPair((list)->{
									List<Tuple2<String, Integer>> myList = new ArrayList<>();
									for(String product1 : list){
										for(String product2 : list){
											if(product1.compareTo(product2)>0)
												myList.add(new Tuple2<String,Integer>(product1+" "+product2, 1));
										}
									}
									return myList.iterator(); //gives back an iterator on the list
								})
								//count the frequencies
								.reduceByKey((val1, val2)->val1+val2)
								//remove pairs that have frequency = 1
								.filter((tuple)->{
									int val = tuple._2();
									if(val>1)
										return true;
									return false;
								})
								//swap key and value
								.mapToPair((tuple)->new Tuple2<Integer, String>(tuple._2(), tuple._1()))
								//sort by the frequency
								.sortByKey(false);

		frequencyOfCouples.saveAsTextFile(outputPath);
		System.out.println(frequencyOfCouples.take(10));
							

		// Close the Spark context
		sc.close();
	}
}
