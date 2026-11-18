package it.polito.bigdata.spark.example;

import scala.Tuple2;

import org.apache.spark.api.java.*;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;

public class SparkDriver {

	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);

		String inputPath;
		String inputPath2;
		Double threshold;
		String outputFolder;

		inputPath = args[0]; //register.csv
		inputPath2 = args[1];
		threshold = Double.parseDouble(args[2]);
		outputFolder = args[3];

		// Create a configuration object and set the name of the application
		SparkConf conf = new SparkConf().setAppName("Spark Lab #7");

		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #7").setMaster("local");

		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");

		JavaRDD<String> registerRDD = sc.textFile(inputPath);
		
		JavaPairRDD<String, CountTotReadingsTotFull> readingsPerPair = registerRDD
							//filter out header line
							.filter(str->!str.startsWith("station\t"))
							//filter out wrong lines (used slots = 0 and free slots = 0)
							.filter((str)->{
								int freeSlots = new Integer(str.split("\t")[3]);
								int usedSlots = new Integer(str.split("\t")[2]);
								if(freeSlots == 0 && usedSlots==0)
									return false;
								else
									return true;
							})
							//map to JavaPairRDD: (stationId Monday-15)->CountTotReadingsTotFull
							.mapToPair((str->{
								//line of register.csv: 23 2008-05-15 19:01:00 5 13
								String stationId = str.split("\t")[0];
								String hour = str.split("\t")[1].split(" ")[1].split(":")[0]; //takes the hour
								String dayOftheWeek = DateTool.DayOfTheWeek(str.split("\t")[1]); 

								CountTotReadingsTotFull count;
								
								Integer freeSlots = new Integer(str.split("\t")[3]);
								Integer usedSlots = new Integer(str.split("\t")[2]);
								if(freeSlots == 0)
									count = new CountTotReadingsTotFull(1, 1);
								else 
									count = new CountTotReadingsTotFull(1, 0);
								return new Tuple2<String, CountTotReadingsTotFull>(stationId+" "+dayOftheWeek+" "+hour, count);
							}))
							//put togheter the pairs of the same day of the week
							.reduceByKey((element1, element2)->{
								return new CountTotReadingsTotFull
										(element1.numReadings + element2.numReadings,
											 element1.numFullReadings + element2.numFullReadings);
							});

		//rdd with criticality value per each pair: (23 Monday 15) -> criticalityValue
		JavaPairRDD<String, Double> aboveThresholdCriticalityPerPairRDD = readingsPerPair
						//Computes the criticality value for each pair (Si,Tj)
						.mapValues(obj->new Double(obj.numFullReadings)/new Double(obj.numReadings))
						//Select only the pairs having a criticality value greater than or equal to threshold
						.filter(tuple->tuple._2()>=threshold);
		
		//Select the most critical timeslot for each station
		//I want to use a reduceByKey, so
		JavaPairRDD<String, String> resultRDD = aboveThresholdCriticalityPerPairRDD
							.mapToPair((tuple)->{
								String stationId = tuple._1().split(" ")[0];
								String dayOfTheWeek = tuple._1().split(" ")[1];
								String hour = tuple._1().split(" ")[2];
								Double criticalityVal = tuple._2();
								Container myCont = new Container(dayOfTheWeek, hour, criticalityVal);
								return new Tuple2<String, Container>(stationId, myCont);
							})
							.reduceByKey((container1, container2)->{
								if(container1.criticalityVal > container2.criticalityVal)
									return container1;
								else if (container1.criticalityVal < container2.criticalityVal)
									return container2;
								else if (Integer.parseInt(container1.hour) < Integer.parseInt(container2.hour))
									return container1;
								else if (Integer.parseInt(container1.hour) > Integer.parseInt(container2.hour))
									return container2;
								else if (container1.dayOfTheWeek.compareTo(container2.dayOfTheWeek) < 0)
									return container1;
								else
									return container2;
							})
							.mapToPair((tuple)->{
								String stationId = tuple._1();
								String dayOfTheWeek = tuple._2().dayOfTheWeek;
								String hour = tuple._2().hour;
								String criticalityVal = tuple._2().criticalityVal.toString();
								return new Tuple2<String, String>(stationId, dayOfTheWeek+" "+hour+" "+criticalityVal);
							})
							;

		//Read and transform into a JavaPairRDD (for the following join) the file stations.csv
		JavaPairRDD<String, String> stationsRDD =
						 sc.textFile(inputPath2)
                            .filter(line -> !line.startsWith("id\t"))
							.mapToPair((line)->{
								String stationId = line.split("\t")[0];
								String coord1 = line.split("\t")[1];
								String coord2 = line.split("\t")[2];
								return new Tuple2<String, String>(stationId, coord1+","+coord2);
							});
		
		//Perform the join
		JavaPairRDD<String, Tuple2<String, String>> finalRDD = resultRDD.join(stationsRDD);

		// Store in resultKML one String, representing a KML marker, for each station 
		// with a critical timeslot 
		JavaRDD<String> resultKML = finalRDD
									.map((tuple)->{
										String stationId = tuple._1();
										String dayOfTheWeek = tuple._2()._1().split(" ")[0];
										String hour = tuple._2()._1().split(" ")[1];
										String criticality = tuple._2()._1().split(" ")[2];
										String coordinates = tuple._2()._2();

										String kmlMarker = "<Placemark><name>" + stationId + "</name>" + "<ExtendedData>"
										+ "<Data name=\"DayWeek\"><value>" + dayOfTheWeek + "</value></Data>"
										+ "<Data name=\"Hour\"><value>" + hour + "</value></Data>"
										+ "<Data name=\"Criticality\"><value>" + criticality + "</value></Data>"
										+ "</ExtendedData>" + "<Point>" + "<coordinates>" + coordinates + "</coordinates>"
										+ "</Point>" + "</Placemark>";

										return kmlMarker;
									});
		  
		// Invoke coalesce(1) to store all data inside one single partition/i.e., in one single output part file
		resultKML.coalesce(1).saveAsTextFile(outputFolder); 
		
		// Close the Spark context
		sc.close();
	}
}
