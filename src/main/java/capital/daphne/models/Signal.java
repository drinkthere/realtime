package capital.daphne.models;

import lombok.Data;

@Data
public class Signal implements Cloneable {
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

    @Override
    public Signal clone() {
        try {
            // 调用 Object 类的 clone 方法
            return (Signal) super.clone();
        } catch (CloneNotSupportedException e) {
            // 如果类没有实现 Cloneable 接口，会抛出 CloneNotSupportedException
            e.printStackTrace();
            return null;
        }
    }
    
    public enum TradeActionType {
        NO_ACTION, BUY, SELL;
    }

    public enum OrderType {
        OPEN, CLOSE;
    }
}
