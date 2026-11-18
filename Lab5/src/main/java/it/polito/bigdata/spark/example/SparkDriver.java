package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
	
public class SparkDriver {
	
	public static void main(String[] args) {

		// The following two lines are used to switch off some verbose log messages
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);


		String inputPath;
		String outputPath;
		String prefix;
		
		inputPath=args[0];
		outputPath=args[1];
		prefix=args[2];

	
		// Create a configuration object and set the name of the application
		SparkConf conf=new SparkConf().setAppName("Spark Lab #5");
		
		// Use the following command to create the SparkConf object if you want to run
		// your application inside Eclipse.
		// Remember to remove .setMaster("local") before running your application on the cluster
		// SparkConf conf=new SparkConf().setAppName("Spark Lab #5").setMaster("local");
		
		// Create a Spark Context object
		JavaSparkContext sc = new JavaSparkContext(conf);

		// print the application ID
		System.out.println("******************************");
		System.out.println("ApplicationId: "+JavaSparkContext.toSparkContext(sc).applicationId());
		System.out.println("******************************");
		
		// Read the content of the input file/folder
		// Each element/string of wordFreqRDD corresponds to one line of the input data 
		// (i.e, one pair "word\tfreq")  
		JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		/* Task 1 */
		JavaRDD<String> filteredLines = wordFreqRDD.filter(line->line.split("\t", 2)[0].startsWith(prefix));
		System.out.println("Number of selected lines: " + filteredLines.count());
		Integer maxVal = filteredLines.map(str -> new Integer(str.split("\t")[1])).fold(0, (val1, val2)->{
			if(val1>val2)
				return val1;
			else
				return val2;	
		});
		System.out.println("Maximum frequency among words starting with '"+ prefix + "': "+maxVal);

		/* Task 2 */
		JavaRDD<String> finalLines = filteredLines.filter(str->new Integer(str.split("\t")[1]) > maxVal * 0.8);
		System.out.println("Number of selected lines: "+finalLines.count());
		finalLines.map(str -> str.split("\t")[0]).saveAsTextFile(outputPath); //saves in output folder

		// Close the Spark context
		sc.close();
	}
}
