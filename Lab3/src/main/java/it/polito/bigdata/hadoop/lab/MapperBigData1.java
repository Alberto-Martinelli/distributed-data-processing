package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
 

            String [] products = value.toString().split(","); //array of strings
            List<String> productsList = new ArrayList<>();
            for(String p : products){
                productsList.add(p);
            }
            productsList.remove(0); //remove the user

            for (String product1 : productsList){
                for(String product2 : productsList){
                    if(product1.compareTo(product2)>0){
                        context.write(new Text(product1+","+product2), new IntWritable(1));
                    }
                }
            }
    }
}
