package it.polito.bigdata.spark.example;


public class Container implements java.io.Serializable {
    public String dayOfTheWeek;
    public String hour;
    public Double criticalityVal;

    public Container(String dayOfTheWeek, String hour, Double criticalityVal) {
        this.dayOfTheWeek = dayOfTheWeek;
        this.hour = hour;
        this.criticalityVal = criticalityVal;
    }


}
