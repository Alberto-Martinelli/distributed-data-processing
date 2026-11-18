package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        TopKVector<WordCountWritable> top100 = new TopKVector<>(100);
		/* Implement the reduce method */
    	for(WordCountWritable wc : values){
            top100.updateWithNewElement(new WordCountWritable(wc));
        }

        for(WordCountWritable wc2 : top100.getLocalTopK()){
            context.write(new Text(wc2.getWord()), new IntWritable(wc2.getCount()));
        }
    }
}
