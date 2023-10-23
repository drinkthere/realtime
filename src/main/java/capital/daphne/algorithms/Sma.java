package capital.daphne.algorithms;

import capital.daphne.AppConfigManager;
import capital.daphne.models.Signal;
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
import java.util.UUID;

public class Sma implements Algorithm {
    private static final Logger logger = LoggerFactory.getLogger(Sma.class);

    private AppConfigManager.AppConfig.AlgorithmConfig ac;
    private Signal.TradeActionType lastAction;

    private LocalDateTime lastBuyDateTime;

    private LocalDateTime lastSellDateTime;

    private LocalDateTime resetDatetime;

    public Sma(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;

        lastAction = Signal.TradeActionType.NO_ACTION;
        lastBuyDateTime = null;
        lastSellDateTime = null;
        resetDatetime = null;

    }

    @Override
    public Signal getSignal(Table inputDf, int position, int maxPosition) {
        try {
            // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
            int numStatsBars = ac.getNumStatsBars();
            String benchmark = "sma" + numStatsBars;
            Table df = addBenchMarkColumn(inputDf, benchmark, numStatsBars);
            Row latestBar = df.row(df.rowCount() - 1);
            double volatility = latestBar.getDouble("volatility");
            double volatilityMultiplier = calToVolatilityMultiplier(volatility);
            // System.out.println(latestBar.getString("date_us") + "|" + latestBar.getDouble("vwap") + "|" + latestBar.getDouble(benchmark) + "|" + volatility + "|" + volatilityMultiplier);
            return processPriceBar(latestBar, volatilityMultiplier, benchmark, position, maxPosition);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("save signal failed, error:" + e.getMessage());
            return null;
        }
    }


    private Table addBenchMarkColumn(Table df, String benchmark, int numStatsBars) {
        DoubleColumn vwap = df.doubleColumn("prev_vwap");
        DoubleColumn sma = vwap.rolling(numStatsBars).mean();
        sma.setName(benchmark);
        df.addColumns(sma);
        return df;
    }

    private double calToVolatilityMultiplier(double volatility) {
        return 1 + ac.getVolatilityA() +
                ac.getVolatilityB() * volatility +
                ac.getVolatilityC() * volatility * volatility;
    }

    private Signal processPriceBar(Row row, double volatilityMultiplier, String benchmarkColumn, int position, int maxPosition) {
        String date_us = row.getString("date_us");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
        LocalDateTime datetime = zonedDateTime.toLocalDateTime();
        LocalTime time = datetime.toLocalTime();

        double[] signalMargins = calculateSignalMargin(volatilityMultiplier, position, maxPosition);
        double buySignalMargin = signalMargins[0];
        double sellSignalMargin = signalMargins[1];
        // System.out.println(buySignalMargin + "|" + sellSignalMargin + "|" + position + "|" + volatilityMultiplier);
        double vwap = row.getDouble("vwap");
        double sma = row.getDouble(benchmarkColumn);

        long buyIntervalSeconds = 0L;
        if (!lastAction.equals(Signal.TradeActionType.NO_ACTION) && lastBuyDateTime != null) {
            Duration buyDuration = Duration.between(lastBuyDateTime, datetime);
            buyIntervalSeconds = buyDuration.getSeconds();
        }
        long sellIntervalSeconds = 0L;
        if (!lastAction.equals(Signal.TradeActionType.NO_ACTION) && lastSellDateTime != null) {
            Duration sellDuration = Duration.between(lastSellDateTime, datetime);
            sellIntervalSeconds = sellDuration.getSeconds();
        }

        double longThreshold = sma * (1 - buySignalMargin);
        double shortThreshold = sma * (1 + sellSignalMargin);

        Signal signal = null;
        if (!ac.getSecType().equals("FUT")) {
            LocalTime portfolioCloseTime = LocalTime.of(15, 50, 0);
            LocalTime marketOpenTime = LocalTime.of(9, 30, 0);
            LocalTime marketCloseTime = LocalTime.of(16, 0, 0);
            LocalTime tradeEndTime = !ac.isPortfolioRequiredToClose() ? marketCloseTime : portfolioCloseTime;

            if ((time.isAfter(marketOpenTime) && time.isBefore(tradeEndTime)) || time.equals(marketOpenTime) || time.equals(tradeEndTime)) {
                logger.info(String.format("%s|%s|%s|place|%f|<=%f|>=%f|%s|%s|%d|%s",
                        ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, longThreshold, shortThreshold, vwap <= longThreshold, vwap >= shortThreshold, position, lastAction));
                if (vwap <= longThreshold
                        && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                        && (position < maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                    signal = fulfillSignal(vwap, ac.getOrderSize(), Signal.OrderType.OPEN);
                    lastAction = Signal.TradeActionType.BUY;
                    lastBuyDateTime = datetime;
                } else if (vwap >= shortThreshold
                        && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                        && (position > -maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                    signal = fulfillSignal(vwap, -ac.getOrderSize(), Signal.OrderType.OPEN);
                    lastAction = Signal.TradeActionType.SELL;
                    lastSellDateTime = datetime;
                } else if (Math.abs(position) >= maxPosition * ac.getHardLimit() && ac.getHardLimitClosePositionMethod().equals("RESET") && lastAction != null) {
                    if (resetDatetime == null) {
                        resetDatetime = datetime.plusSeconds(ac.getMinDurationWhenReset());
                        if (resetDatetime.toLocalTime().isAfter(tradeEndTime)) {
                            long durationSeconds = Duration.between(tradeEndTime, resetDatetime.toLocalTime()).getSeconds();
                            resetDatetime = resetDatetime.minusSeconds(Math.abs(durationSeconds));
                        }
                    }
                    if (datetime.isAfter(resetDatetime) || datetime.equals(resetDatetime)) {
                        signal = fulfillSignal(vwap, -position, Signal.OrderType.CLOSE);
                        lastAction = null;
                        lastBuyDateTime = null;
                        lastSellDateTime = null;
                        resetDatetime = null;
                    }
                }
            } else if ((time.isAfter(portfolioCloseTime) && time.isBefore(marketCloseTime) || time.equals(portfolioCloseTime) || time.equals(marketCloseTime))
                    && ac.isPortfolioRequiredToClose()) {
                signal = fulfillSignal(vwap, -position, Signal.OrderType.CLOSE);
                lastAction = null;
                lastBuyDateTime = null;
                lastSellDateTime = null;
                resetDatetime = null;
            } else {
                lastAction = null;
                lastBuyDateTime = null;
                lastSellDateTime = null;
                resetDatetime = null;
            }
        } else {
            // FUT交易不受时间限制
            logger.info(String.format("%s|%s|%s|place|%f|<=%f|>=%f|%s|%s|%d",
                    ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, longThreshold, shortThreshold, vwap <= longThreshold, vwap >= shortThreshold, position));

            if (vwap <= longThreshold
                    && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                    && ((position + ac.getOrderSize()) <= maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                signal = fulfillSignal(vwap, ac.getOrderSize(), Signal.OrderType.OPEN);
                lastAction = Signal.TradeActionType.BUY;
                lastBuyDateTime = datetime;
            } else if (vwap >= shortThreshold
                    && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                    && ((position - ac.getOrderSize()) >= -maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                signal = fulfillSignal(vwap, -ac.getOrderSize(), Signal.OrderType.OPEN);
                lastAction = Signal.TradeActionType.SELL;
                lastSellDateTime = datetime;
            } else if (Math.abs(position) >= maxPosition * ac.getHardLimit() && ac.getHardLimitClosePositionMethod().equals("RESET") && lastAction != null) {
                if (resetDatetime == null) {
                    resetDatetime = datetime.plusSeconds(ac.getMinDurationWhenReset());
                }
                if (datetime.isAfter(resetDatetime) || datetime.equals(resetDatetime)) {
                    signal = fulfillSignal(vwap, -position, Signal.OrderType.CLOSE);
                    lastAction = null;
                    lastBuyDateTime = null;
                    lastSellDateTime = null;
                    resetDatetime = null;
                }
            }
        }
        return signal;
    }

    private double[] calculateSignalMargin(double volatilityMultiplier, int position, int maxPosition) {
        float signalMargin = ac.getSignalMargin();
        float positionSignalMarginOffset = ac.getPositionSignalMarginOffset();
        if (!ac.getSecType().equals("FUT")) {
            position = position / 100;
        }

        double buySignalMargin = (signalMargin + positionSignalMarginOffset * position) * volatilityMultiplier;
        double sellSignalMargin = (signalMargin - positionSignalMarginOffset * position) * volatilityMultiplier;
        return new double[]{buySignalMargin, sellSignalMargin};
    }

    private Signal fulfillSignal(double vwap, int position, Signal.OrderType orderType) {
        Signal signal = new Signal();
        signal.setValid(true);
        signal.setAccountId(ac.getAccountId());
        signal.setUuid(UUID.randomUUID().toString());
        signal.setSymbol(ac.getSymbol());
        signal.setSecType(ac.getSecType());
        signal.setWap(vwap);
        signal.setQuantity(position);
        signal.setOrderType(orderType);
        return signal;
    }
}
