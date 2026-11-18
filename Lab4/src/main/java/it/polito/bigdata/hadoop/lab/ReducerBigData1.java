package it.polito.bigdata.hadoop.lab;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                ProductAndScore,    // Input value type
                Text,           // Output key type
                DoubleWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<ProductAndScore> values, // Input value type
        Context context) throws IOException, InterruptedException {
        List<ProductAndScore> pList = new ArrayList<>();

        Double sum = 0d;
        int occurrences = 0;
		for(ProductAndScore ps : values){
            sum += ps.getScore();
            occurrences++;
            pList.add(new ProductAndScore(ps.getProductId(), ps.getScore()));
        }
        Double mean;
        if(occurrences==0)
            mean = 0d;
        else
            mean = sum/occurrences;

        Double val;
        for(ProductAndScore ps : pList){
            val = ps.getScore() - mean;
            context.write(new Text(ps.getProductId()), new DoubleWritable(val));
        }
    }
}
