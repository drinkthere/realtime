package capital.daphne.algorithms;

import capital.daphne.AppConfigManager;
import capital.daphne.models.ActionInfo;
import capital.daphne.models.Signal;
import capital.daphne.services.BarSvc;
import capital.daphne.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class EMA implements AlgorithmProcessor {
    private static final Logger logger = LoggerFactory.getLogger(EMA.class);

    private AppConfigManager.AppConfig.AlgorithmConfig ac;

    private LocalDateTime resetDatetime;

    private String benchmarkColumnName;

    private BarSvc barSvc;

    public EMA(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
        resetDatetime = null;
        barSvc = new BarSvc();
    }

    @Override
    public Signal getSignal(Table inputDf, int position, int maxPosition) {
        try {
            Table df = preProcess(inputDf);
            Row latestBar = df.row(df.rowCount() - 1);
            return processToGetSignal(latestBar, position, maxPosition);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("save signal failed, error:" + e.getMessage());
            return null;
        }
    }

    private Table preProcess(Table df) {
        // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
        int numStatsBars = ac.getNumStatsBars();
        benchmarkColumnName = ac.getName() + numStatsBars;
        DoubleColumn prevWapCol = df.doubleColumn("prev_vwap");
        DoubleColumn emaColumn = DoubleColumn.create(benchmarkColumnName, prevWapCol.size());


        int period = numStatsBars;
        double multiplier = 2.0 / (period + 1);
        Double prevEma = barSvc.getEma(ac.getAccountId(), ac.getSymbol(), ac.getSecType());
        Double ema = prevWapCol.get(prevWapCol.size() - 1) * multiplier + prevEma * (1 - multiplier);
        // System.out.println(df.row(df.rowCount() - 1).getString("date_us") + "|" + ema);
        emaColumn.set(emaColumn.size() - 1, ema);
        barSvc.setEma(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), ema);

        df.addColumns(emaColumn);
        logger.debug("====" + ema + "==" + emaColumn.get(emaColumn.size() - 1) + "==" + df.row(df.rowCount() - 1).getDouble(benchmarkColumnName));
        return df;
    }

    private Signal processToGetSignal(Row row, int position, int maxPosition) {
        double volatility = row.getDouble("volatility");

        double volatilityMultiplier = Utils.calToVolatilityMultiplier(ac, volatility);
        LocalDateTime lastSellDateTime = null;
        LocalDateTime lastBuyDateTime = null;

        String redisKey = String.format("%s:%s:%s:%s:LAST_ACTION", ac.getAccountId(), ac.getSymbol(), ac.getSecType(), benchmarkColumnName);
        ActionInfo lastActionInfo = Utils.getLastActionInfo(redisKey);
        logger.info(redisKey + "|" + lastActionInfo);

        Signal.TradeActionType lastAction = lastActionInfo.getAction();
        if (lastAction.equals(Signal.TradeActionType.BUY)) {
            lastBuyDateTime = lastActionInfo.getDateTime();
        } else if (lastAction.equals(Signal.TradeActionType.SELL)) {
            lastSellDateTime = lastActionInfo.getDateTime();
        }

        LocalDateTime datetime = Utils.genUsDateTime(row.getString("date_us"), "yyyy-MM-dd HH:mm:ssXXX");
        LocalTime time = datetime.toLocalTime();

        double[] signalMargins = Utils.calculateSignalMargin(ac.getSecType(), ac.getSignalMargin(), ac.getPositionSignalMarginOffset(), volatilityMultiplier, position);
        double sellSignalMargin = signalMargins[1];
        double buySignalMargin = signalMargins[0];
        double vwap = row.getDouble("vwap");
        double ema = row.getDouble(benchmarkColumnName);

        long sellIntervalSeconds = 0L;
        if (!lastAction.equals(Signal.TradeActionType.NO_ACTION) && lastSellDateTime != null) {
            Duration sellDuration = Duration.between(lastSellDateTime, datetime);
            sellIntervalSeconds = sellDuration.getSeconds();
        }

        long buyIntervalSeconds = 0L;
        if (!lastAction.equals(Signal.TradeActionType.NO_ACTION) && lastBuyDateTime != null) {
            Duration buyDuration = Duration.between(lastBuyDateTime, datetime);
            buyIntervalSeconds = buyDuration.getSeconds();
        }

        double longThreshold = ema * (1 - buySignalMargin);
        double shortThreshold = ema * (1 + sellSignalMargin);

//        System.out.println(row.getString("date_us") + "|" + vwap + "|" + ema + "|" + volatility + "|" + volatilityMultiplier + "|" + buySignalMargin + "|" + sellSignalMargin + "|" + longThreshold + "|" + shortThreshold
//                + "|" + (vwap <= longThreshold && position < maxPosition) + "|" + (vwap >= shortThreshold && position > -maxPosition));
        Signal signal = null;
        logger.info(String.format("%s|%s|%s|place|%f|<=%f|>=%f|%s|%s|%d|%s",
                ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, longThreshold, shortThreshold, vwap <= longThreshold, vwap >= shortThreshold, position, lastAction));
        if (vwap <= longThreshold
                && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                && (position < maxPosition) && resetDatetime == null) {
            signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, ac.getOrderSize(), Signal.OrderType.OPEN, benchmarkColumnName);
        } else if (vwap >= shortThreshold
                && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                && (position > -maxPosition) && resetDatetime == null) {
            signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, -ac.getOrderSize(), Signal.OrderType.OPEN, benchmarkColumnName);
        }
        return signal;
    }
}
