package capital.daphne.datasource;

import capital.daphne.strategy.Strategy;
import lombok.Data;

@Data
public class Signal {
    // 是否有效
    private boolean valid;
    private String uuid;
    private String symbol;
    private String secType;
    private double bidPrice;
    private double askPrice;
    private double price;
    private double wap;
    private Strategy.TradeActionType side;
    private int quantity;
}
