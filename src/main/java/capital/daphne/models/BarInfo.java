package capital.daphne.models;

import lombok.Data;

@Data
public class BarInfo {
    private String date;
    private double vwap;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volatility;
}