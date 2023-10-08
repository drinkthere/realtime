package capital.daphne.strategy;

import tech.tablesaw.api.Table;

public interface Strategy {

    public TradeActionType getSignalSide(Table df, int position, int maxPosition);

    public enum TradeActionType {
        NO_ACTION, BUY, SELL;
    }
}
