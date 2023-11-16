package capital.daphne.services.ta;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;

public class WILLR implements TA {
    private static final Logger logger = LoggerFactory.getLogger(WILLR.class);

    @Override
    public Table ta(Table df) {
        // 计算DPO
        int period = 990;
        int offset = 1;

        DoubleColumn closePrices = df.doubleColumn("close");
        DoubleColumn highPrices = df.doubleColumn("high");
        DoubleColumn lowPrices = df.doubleColumn("low");

        DoubleColumn willrColumn = willr(closePrices, highPrices, lowPrices, period, offset);
        df.addColumns(willrColumn.setName("WILLR"));
        return df;
    }

    private DoubleColumn willr(DoubleColumn closePrices, DoubleColumn highPrices, DoubleColumn lowPrices, int period, int offset) {
        DoubleColumn highestHigh = highPrices.rolling(period).max();
        DoubleColumn lowestLow = lowPrices.rolling(period).min();
        DoubleColumn historyRange = highestHigh.subtract(lowestLow);
        DoubleColumn currentRange = (closePrices.subtract(lowestLow));
        return currentRange.divide(historyRange).subtract(1).multiply(100).asDoubleColumn().lag(offset);
    }
}
