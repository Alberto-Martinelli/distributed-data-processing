package it.polito.bigdata.hadoop.lab;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


public class ProductAndScore implements Writable {
    private String productId;
    private Double score;
    
    public ProductAndScore(String productId, Double score){
        this.productId = productId;
        this.score = score;
    }

    /*A DEFAULT CONSTRUCTOR IS MANDATORY */
    public ProductAndScore(){
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Double getScore() {
        return score;
    }

    public void setScore(Double score) {
        this.score = score;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeUTF(productId);
        out.writeDouble(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        productId = in.readUTF();
        score = in.readDouble();
    }

    
    public String toString(){
        return new String(productId+"-"+score);
    }

}
