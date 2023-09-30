package capital.daphne.strategy;

import capital.daphne.AppConfig;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class Sma implements Strategy {
    private AppConfig config;
    private TradeActionType lastAction = TradeActionType.NO_ACTION;
    private LocalDateTime lastBuyDatetime;
    private LocalDateTime lastSellDatetime;

    public Sma(AppConfig conf) {
        config = conf;
    }

    @Override
    public TradeActionType getSignalSide(String symbol, Table inputDf, int position) {
        // 生成关键指标，这里是sma12
        String benchmark = "sma" + config.getNumStatsBars();
        Table df = addBenchMarkColumn(inputDf, benchmark);
        Row latestBar = df.row(df.rowCount() - 1);

        double volatility = latestBar.getDouble("volatility");
        double volatilityMultiplier = calToVolatilityMultiplier(volatility);

        TradeActionType action = processPriceBar(latestBar, volatilityMultiplier, position, benchmark);

        return action;
    }

    private Table addBenchMarkColumn(Table df, String benchmark) {
        DoubleColumn vwap = df.doubleColumn("prev_vwap");
        DoubleColumn sma = vwap.rolling(config.getNumStatsBars()).mean();
        sma.setName(benchmark);
        df.addColumns(sma);
        return df;
    }

    private double calToVolatilityMultiplier(double volatility) {
        return 1 + config.getVolatilityA() + config.getVolatilityB() * volatility + config.getVolatilityC() * volatility * volatility;
    }

    private TradeActionType processPriceBar(Row row, double volatilityMultiplier, int position, String benchmarkColumn) {
        TradeActionType action = TradeActionType.NO_ACTION;
        LocalTime portfolioCloseTime = LocalTime.of(15, 50, 0);
        LocalTime marketOpenTime = LocalTime.of(9, 30, 0);
        LocalTime marketCloseTime = LocalTime.of(16, 0, 0);

        LocalTime tradeEndTime = !config.isPortfolioRequiredToClose() ? marketCloseTime : portfolioCloseTime;

        String date_us = row.getString("date_us");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
        LocalDateTime datetime = zonedDateTime.toLocalDateTime();
        LocalTime time = datetime.toLocalTime();

        // System.out.println(portfolioPositions);
        double[] signalMargins = calculateSignalMargin(volatilityMultiplier, position);
        double buySignalMargin = signalMargins[0];
        double sellSignalMargin = signalMargins[1];

        if ((time.isAfter(marketOpenTime) && time.isBefore(tradeEndTime)) || time.equals(marketOpenTime) || time.equals(tradeEndTime)) {
            double vwap = row.getDouble("vwap");

            double sma12 = row.getDouble(benchmarkColumn);
            long buyIntervalSeconds = 0L;
            if (lastAction != TradeActionType.NO_ACTION && lastBuyDatetime != null) {
                Duration buyDuration = Duration.between(lastBuyDatetime, datetime);
                buyIntervalSeconds = buyDuration.getSeconds();
            }
            long sellIntervalSeconds = 0L;
            if (lastAction != TradeActionType.NO_ACTION && lastSellDatetime != null) {
                Duration sellDuration = Duration.between(lastSellDatetime, datetime);
                sellIntervalSeconds = sellDuration.getSeconds();
            }

            if (vwap <= sma12 * (1 - buySignalMargin)
                    && (lastAction == TradeActionType.NO_ACTION || lastAction.equals(TradeActionType.SELL) || buyIntervalSeconds >= config.getMinIntervalBetweenSignal())
                    && ((position < config.getMaxPortfolioPositions() && config.isHardLimit()) || !config.isHardLimit())) {
                action = TradeActionType.BUY;
                lastAction = TradeActionType.BUY;
                lastBuyDatetime = datetime;

            } else if (vwap >= sma12 * (1 + sellSignalMargin)
                    && (lastAction == TradeActionType.NO_ACTION || lastAction.equals(TradeActionType.BUY) || sellIntervalSeconds >= config.getMinIntervalBetweenSignal())
                    && ((position > (-config.getMaxPortfolioPositions()) && config.isHardLimit()) || config.isHardLimit())) {
                action = TradeActionType.SELL;
                lastAction = TradeActionType.SELL;
                lastSellDatetime = datetime;
            }
        } else {
            lastAction = TradeActionType.NO_ACTION;
            lastBuyDatetime = null;
            lastSellDatetime = null;
        }
        return action;
    }

    public double[] calculateSignalMargin(double volatilityMultiplier, int position) {
        float signalMargin = config.getSignalMargin();
        int maxPortfolioPositions = config.getMaxPortfolioPositions();
        float positionSignalMarginOffset = config.getPositionSignalMarginOffset();

        float positionOffset = position / maxPortfolioPositions;

        double buySignalMargin = (signalMargin + positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        double sellSignalMargin = (signalMargin - positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        return new double[]{buySignalMargin, sellSignalMargin};
    }
}
