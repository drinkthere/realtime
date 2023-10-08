package capital.daphne.strategy;

import capital.daphne.AppConfig;
import capital.daphne.datasource.ibkr.Ibkr;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Sma implements Strategy {
    private static final Logger logger = LoggerFactory.getLogger(Ibkr.class);

    private final AppConfig.SymbolConfig sc;

    private final AppConfig.Strategy strategyConf;

    private TradeActionType lastAction;

    private LocalDateTime lastBuyDateTime;

    private LocalDateTime lastSellDateTime;

    public Sma(AppConfig.SymbolConfig symbolConfig) {
        sc = symbolConfig;
        strategyConf = sc.getStrategy();

        lastAction = TradeActionType.NO_ACTION;
        lastBuyDateTime = null;
        lastSellDateTime = null;
    }

    @Override
    public TradeActionType getSignalSide(Table inputDf, int position, int maxPosition) {
        // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
        int numStatsBars = strategyConf.getNumStatsBars();
        String benchmark = "sma" + numStatsBars;
        Table df = addBenchMarkColumn(inputDf, benchmark, numStatsBars);
        Row latestBar = df.row(df.rowCount() - 1);

        double volatility = latestBar.getDouble("volatility");
        double volatilityMultiplier = calToVolatilityMultiplier(volatility);
        return processPriceBar(latestBar, volatilityMultiplier, benchmark, position, maxPosition);
    }

    private Table addBenchMarkColumn(Table df, String benchmark, int numStatsBars) {
        DoubleColumn vwap = df.doubleColumn("prev_vwap");
        DoubleColumn sma = vwap.rolling(numStatsBars).mean();
        sma.setName(benchmark);
        df.addColumns(sma);
        return df;
    }

    private double calToVolatilityMultiplier(double volatility) {
        return 1 + strategyConf.getVolatilityA() +
                strategyConf.getVolatilityB() * volatility +
                strategyConf.getVolatilityC() * volatility * volatility;
    }

    synchronized private TradeActionType processPriceBar(Row row, double volatilityMultiplier, String benchmarkColumn, int position, int maxPosition) {
        TradeActionType action;
        LocalTime portfolioCloseTime = LocalTime.of(15, 50, 0);
        LocalTime marketOpenTime = LocalTime.of(9, 30, 0);
        LocalTime marketCloseTime = LocalTime.of(16, 0, 0);

        LocalTime tradeEndTime = !strategyConf.isPortfolioRequiredToClose() ? marketCloseTime : portfolioCloseTime;

        String date_us = row.getString("date_us");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
        LocalDateTime datetime = zonedDateTime.toLocalDateTime();
        LocalTime time = datetime.toLocalTime();


        double[] signalMargins = calculateSignalMargin(volatilityMultiplier, position, maxPosition);
        double buySignalMargin = signalMargins[0];
        double sellSignalMargin = signalMargins[1];

        if ((time.isAfter(marketOpenTime) && time.isBefore(tradeEndTime)) || time.equals(marketOpenTime) || time.equals(tradeEndTime) || sc.getSecType().equals("FUT")) {
            double vwap = row.getDouble("vwap");

            double sma = row.getDouble(benchmarkColumn);
            long buyIntervalSeconds = 0L;
            if (!lastAction.equals(TradeActionType.NO_ACTION) && lastBuyDateTime != null) {
                Duration buyDuration = Duration.between(lastBuyDateTime, datetime);
                buyIntervalSeconds = buyDuration.getSeconds();
            }
            long sellIntervalSeconds = 0L;
            if (!lastAction.equals(TradeActionType.NO_ACTION) && lastSellDateTime != null) {
                Duration sellDuration = Duration.between(lastSellDateTime, datetime);
                sellIntervalSeconds = sellDuration.getSeconds();
            }
            logger.info(String.format("symbol=%s, secType=%s, vwap=%f, sma=%f, vwap<=sma*(1-buySignalMargin):%s, vwap >= sma * (1 + sellSignalMargin)：%s",
                    sc.getSymbol(), sc.getSecType(), vwap, sma, vwap <= sma * (1 - buySignalMargin), vwap >= sma * (1 + sellSignalMargin)));
            if (vwap <= sma * (1 - buySignalMargin)
                    && (lastAction.equals(TradeActionType.NO_ACTION) || lastAction.equals(TradeActionType.SELL) || buyIntervalSeconds >= strategyConf.getMinIntervalBetweenSignal())
                    && (position < maxPosition || !strategyConf.isHardLimit())) {
                action = TradeActionType.BUY;
                lastAction = TradeActionType.BUY;
                lastBuyDateTime = datetime;

            } else if (vwap >= sma * (1 + sellSignalMargin)
                    && (lastAction.equals(TradeActionType.NO_ACTION) || lastAction.equals(TradeActionType.BUY) || sellIntervalSeconds >= strategyConf.getMinIntervalBetweenSignal())
                    && (position > (-maxPosition) || !strategyConf.isHardLimit())) {
                action = TradeActionType.SELL;
                lastAction = TradeActionType.SELL;
                lastSellDateTime = datetime;
            } else {
                action = TradeActionType.NO_ACTION;
            }
        } else {
            action = TradeActionType.NO_ACTION;
            lastAction = TradeActionType.NO_ACTION;
            lastBuyDateTime = null;
            lastSellDateTime = null;
        }

        return action;
    }

    private double[] calculateSignalMargin(double volatilityMultiplier, int position, int maxPosition) {
        float signalMargin = strategyConf.getSignalMargin();
        float positionSignalMarginOffset = strategyConf.getPositionSignalMarginOffset();

        float positionOffset = position / (float) maxPosition;
        double buySignalMargin = (signalMargin + positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        double sellSignalMargin = (signalMargin - positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        return new double[]{buySignalMargin, sellSignalMargin};
    }
}
