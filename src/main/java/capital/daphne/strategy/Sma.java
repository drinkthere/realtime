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
        while (true) {
            // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
            int numStatsBars = strategyConf.getNumStatsBars();

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
                            sc.getSymbol(), sc.getSecType(), vwap, threshold, vwap >= threshold));
                    if (vwap >= threshold && (lastAction.equals(TradeActionType.BUY) || sellIntervalSeconds >= strategyConf.getMinIntervalBetweenSignal())) {
                        lastAction = TradeActionType.SELL;
                        lastSellDateTime = latestBarTime;
                        return lastAction;
                    }
                } else if (buyIntervalSeconds < 150) {
                    numStatsBars = (int) Math.round(numStatsBars / 1.5);
                    double[] barData = genLatestBarData(numStatsBars, inputDf);
                    double vwap = barData[0];
                    double threshold = barData[1] * (1 + 0.0002); //暂时写死这个
                    logger.info(String.format("%s|%s|closeLong|x<150|%f|%f|%s",
                            sc.getSymbol(), sc.getSecType(), vwap, threshold, vwap >= threshold));
                    if (vwap >= threshold && (lastAction.equals(TradeActionType.BUY) || sellIntervalSeconds >= strategyConf.getMinIntervalBetweenSignal())) {
                        lastAction = TradeActionType.SELL;
                        lastSellDateTime = latestBarTime;
                        return lastAction;
                    }
                }
                break;
            } else if (position < 0) {
                // 没有上次做空记录，但有残留仓位，跳出循环，走默认处理流程
                if (lastSellDateTime == null) {
                    logger.info(String.format("%s.%s has legacy position = %d, route to default processing",
                            sc.getSymbol(), sc.getSecType(), position));
                    break;
                }

                if (sellIntervalSeconds < 30) {
                    numStatsBars = (int) Math.round(numStatsBars / 2.0);
                    double[] barData = genLatestBarData(numStatsBars, inputDf);
                    double vwap = barData[0];
                    double threshold = barData[1] * (1 - 0.0001); //暂时写死这个
                    logger.info(String.format("%s|%s|closeShort|<30|%f|%f|%s",
                            sc.getSymbol(), sc.getSecType(), vwap, threshold, vwap <= threshold));
                    if (vwap <= threshold && (lastAction.equals(TradeActionType.SELL) || buyIntervalSeconds >= strategyConf.getMinIntervalBetweenSignal())) {
                        lastAction = TradeActionType.BUY;
                        lastBuyDateTime = latestBarTime;
                        return lastAction;
                    }
                } else if (sellIntervalSeconds < 150) {
                    numStatsBars = (int) Math.round(numStatsBars / 1.5);
                    double[] barData = genLatestBarData(numStatsBars, inputDf);
                    double vwap = barData[0];
                    double threshold = barData[1] * (1 - 0.0002); //暂时写死这个
                    logger.info(String.format("%s|%s|closeShort|<30|%f|%f|%s",
                            sc.getSymbol(), sc.getSecType(), vwap, threshold, vwap <= threshold));
                    if (vwap <= threshold && (lastAction.equals(TradeActionType.SELL) || buyIntervalSeconds >= strategyConf.getMinIntervalBetweenSignal())) {
                        lastBuyDateTime = latestBarTime;
                        lastAction = TradeActionType.BUY;
                        return lastAction;
                    }
                }
                break;
            }
            break;
        }

        // 走默认流程
        // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
        int numStatsBars = strategyConf.getNumStatsBars();
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

            double longThreshold = sma * (1 - buySignalMargin);
            double shortThreshold = sma * (1 + sellSignalMargin);
            logger.info(String.format("%s|%s|place|%f|<=%f|>=%f|%s|%s",
                    sc.getSymbol(), sc.getSecType(), vwap, longThreshold, shortThreshold, vwap <= longThreshold, vwap >= shortThreshold));
            if (vwap <= longThreshold
                    && (lastAction.equals(TradeActionType.NO_ACTION) || lastAction.equals(TradeActionType.SELL) || buyIntervalSeconds >= strategyConf.getMinIntervalBetweenSignal())
                    && (position < maxPosition || !strategyConf.isHardLimit())) {
                action = TradeActionType.BUY;
                lastAction = TradeActionType.BUY;
                lastBuyDateTime = datetime;

            } else if (vwap >= shortThreshold
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
