package capital.daphne.strategy;

import tech.tablesaw.api.Table;

public interface Strategy {

    public TradeActionType getSignalSide(String symbol, String secType, Table df, int position);

    public enum TradeActionType {
        NO_ACTION, BUY, SELL;
    }
}
