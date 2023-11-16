package testmodels;

import lombok.Data;

@Data
public class Bar {
    private String date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;
    private double vwap;
    private int barCount;
}