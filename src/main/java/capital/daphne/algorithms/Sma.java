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
        // 注释掉的部分为提钱平仓的测试逻辑，暂时保留一段时间
        /*
        while (true) {
            // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
            int numStatsBars = ac.getNumStatsBars();

            // 先获取最新一行记录，得到当前bar时间，用来和上一次成交时间进行对比
            Row row = inputDf.row(inputDf.rowCount() - 1);
            String date_us = row.getString("date_us");
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
            ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
            LocalDateTime latestBarTime = zonedDateTime.toLocalDateTime();

            long buyIntervalSeconds = 0L;
            long sellIntervalSeconds = 0L;
            if (lastBuyDateTime != null) {
                Duration buyDuration = Duration.between(lastBuyDateTime, latestBarTime);
                buyIntervalSeconds = buyDuration.getSeconds();
            }

            if (lastSellDateTime != null) {
                Duration sellDuration = Duration.between(lastSellDateTime, latestBarTime);
                sellIntervalSeconds = sellDuration.getSeconds();
            }

            if (position > 0) {
                // 没有上次购买记录，有残留仓位，跳出循环，走默认处理流程
                if (lastBuyDateTime == null) {
                    logger.info(String.format("%s.%s has legacy position = %d, route to default processing",
                            sc.getSymbol(), sc.getSecType(), position));
                    break;
                }

                // 当前时间跟上一次买单时间对比
                if (buyIntervalSeconds < 30) {
                    numStatsBars = (int) Math.round(numStatsBars / 2.0);
                    double[] barData = genLatestBarData(numStatsBars, inputDf);
                    double vwap = barData[0];
                    double threshold = barData[1] * (1 + 0.0001); //暂时写死这个
                    logger.info(String.format("%s|%s|closeLong|<30|%f|%f|%s",
                            ac.getSymbol(), ac.getSecType(), vwap, threshold, vwap >= threshold));
                    if (vwap >= threshold && (lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())) {
                        lastAction = Signal.TradeActionType.SELL;
                        lastSellDateTime = latestBarTime;
                        return lastAction;
                    }
                } else if (buyIntervalSeconds < 150) {
                    numStatsBars = (int) Math.round(numStatsBars / 1.5);
                    double[] barData = genLatestBarData(numStatsBars, inputDf);
                    double vwap = barData[0];
                    double threshold = barData[1] * (1 + 0.0002); //暂时写死这个
                    logger.info(String.format("%s|%s|closeLong|x<150|%f|%f|%s",
                            ac.getSymbol(), ac.getSecType(), vwap, threshold, vwap >= threshold));
                    if (vwap >= threshold && (lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())) {
                        lastAction = Signal.TradeActionType.SELL;
                        lastSellDateTime = latestBarTime;
                        return lastAction;
                    }
                }
                break;
            } else if (position < 0) {
                // 没有上次做空记录，但有残留仓位，跳出循环，走默认处理流程
                if (lastSellDateTime == null) {
                    logger.info(String.format("%s.%s has legacy position = %d, route to default processing",
                            ac.getSymbol(), ac.getSecType(), position));
                    break;
                }

                if (sellIntervalSeconds < 30) {
                    numStatsBars = (int) Math.round(numStatsBars / 2.0);
                    double[] barData = genLatestBarData(numStatsBars, inputDf);
                    double vwap = barData[0];
                    double threshold = barData[1] * (1 - 0.0001); //暂时写死这个
                    logger.info(String.format("%s|%s|closeShort|<30|%f|%f|%s",
                            ac.getSymbol(), ac.getSecType(), vwap, threshold, vwap <= threshold));
                    if (vwap <= threshold && (lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())) {
                        lastAction = Signal.TradeActionType.BUY;
                        lastBuyDateTime = latestBarTime;
                        return lastAction;
                    }
                } else if (sellIntervalSeconds < 150) {
                    numStatsBars = (int) Math.round(numStatsBars / 1.5);
                    double[] barData = genLatestBarData(numStatsBars, inputDf);
                    double vwap = barData[0];
                    double threshold = barData[1] * (1 - 0.0002); //暂时写死这个
                    logger.info(String.format("%s|%s|closeShort|<30|%f|%f|%s",
                            ac.getSymbol(), ac.getSecType(), vwap, threshold, vwap <= threshold));
                    if (vwap <= threshold && (lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())) {
                        lastBuyDateTime = latestBarTime;
                        lastAction = Signal.TradeActionType.BUY;
                        return lastAction;
                    }
                }
                break;
            }
            break;
        }
        */

        // 走默认流程
        // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
        int numStatsBars = ac.getNumStatsBars();
        String benchmark = "sma" + numStatsBars;
        Table df = addBenchMarkColumn(inputDf, benchmark, numStatsBars);
        Row latestBar = df.row(df.rowCount() - 1);
        double volatility = latestBar.getDouble("volatility");
        double volatilityMultiplier = calToVolatilityMultiplier(volatility);
        return processPriceBar(latestBar, volatilityMultiplier, benchmark, position, maxPosition);
    }

    private double[] genLatestBarData(int numStatsBars, Table inputDf) {
        String benchmark = "sma" + numStatsBars;
        Table df = addBenchMarkColumn(inputDf, benchmark, numStatsBars);
        Row latestBar = df.row(df.rowCount() - 1);
        double vwap = latestBar.getDouble("vwap");
        double sma = latestBar.getDouble(benchmark);

        return new double[]{vwap, sma};
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
                logger.info(String.format("%s|%s|%s|place|%f|<=%f|>=%f|%s|%s",
                        ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, longThreshold, shortThreshold, vwap <= longThreshold, vwap >= shortThreshold));
                if (vwap <= longThreshold
                        && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                        && (position < maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                    signal = fulfillSignal(vwap, ac.getOrderSize());
                    lastAction = Signal.TradeActionType.BUY;
                    lastBuyDateTime = datetime;
                } else if (vwap >= shortThreshold
                        && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                        && (position > -maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                    signal = fulfillSignal(vwap, -ac.getOrderSize());
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
                        signal = fulfillSignal(vwap, -position);
                        lastAction = null;
                        lastBuyDateTime = null;
                        lastSellDateTime = null;
                        resetDatetime = null;
                    }
                }
            } else if ((time.isAfter(portfolioCloseTime) && time.isBefore(marketCloseTime) || time.equals(portfolioCloseTime) || time.equals(marketCloseTime))
                    && ac.isPortfolioRequiredToClose()) {
                signal = fulfillSignal(vwap, -position);
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
            logger.info(String.format("%s|%s|%s|place|%f|<=%f|>=%f|%s|%s",
                    ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, longThreshold, shortThreshold, vwap <= longThreshold, vwap >= shortThreshold));

            if (vwap <= longThreshold
                    && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                    && (position < maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                signal = fulfillSignal(vwap, ac.getOrderSize());
                lastAction = Signal.TradeActionType.BUY;
                lastBuyDateTime = datetime;
            } else if (vwap >= shortThreshold
                    && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                    && (position > -maxPosition * ac.getHardLimit()) && resetDatetime == null) {
                signal = fulfillSignal(vwap, -ac.getOrderSize());
                lastAction = Signal.TradeActionType.SELL;
                lastSellDateTime = datetime;
            } else if (Math.abs(position) >= maxPosition * ac.getHardLimit() && ac.getHardLimitClosePositionMethod().equals("RESET") && lastAction != null) {
                if (resetDatetime == null) {
                    resetDatetime = datetime.plusSeconds(ac.getMinDurationWhenReset());
                }
                if (datetime.isAfter(resetDatetime) || datetime.equals(resetDatetime)) {
                    signal = fulfillSignal(vwap, -position);
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

        float positionOffset = position / (float) maxPosition;
        double buySignalMargin = (signalMargin + positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        double sellSignalMargin = (signalMargin - positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        return new double[]{buySignalMargin, sellSignalMargin};
    }

    private Signal fulfillSignal(double vwap, int position) {
        Signal signal = new Signal();
        signal.setValid(true);
        signal.setAccountId(ac.getAccountId());
        signal.setUuid(UUID.randomUUID().toString());
        signal.setSymbol(ac.getSymbol());
        signal.setSecType(ac.getSecType());
        signal.setWap(vwap);
        signal.setQuantity(position);
        return signal;
    }
}
