package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import it.polito.bigdata.hadoop.lab.DriverBigData.COUNTERS_WORDS;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    private String prefix;

    protected void setup(Context context){
        prefix = context.getConfiguration().get("prefix");
    }

    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            String keyString = key.toString();
            if(keyString.startsWith(prefix)){
                context.write(key, new IntWritable(new Integer(value.toString())));
                context.getCounter(COUNTERS_WORDS.SELECTED_WORDS).increment(1); 
            }
            else
                context.getCounter(COUNTERS_WORDS.DISCARDED_WORDS).increment(1);
            
    }
}
