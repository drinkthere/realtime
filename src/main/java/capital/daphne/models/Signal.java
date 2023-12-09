package capital.daphne.models;

import lombok.Data;

@Data
public class Signal {
    // 是否有效
    private boolean valid;
    private String accountId;
    private String uuid;
    private String symbol;
    private String secType;
    private double wap;
    private int quantity;
    private OrderType orderType;
    private String benchmarkColumn;
    private String optionRight = "";
    private boolean gtd = false;
    private int gtdSec = 0;


    public enum TradeActionType {
        NO_ACTION, BUY, SELL;
    }

    public enum OrderType {
        OPEN, CLOSE;
    }
}
