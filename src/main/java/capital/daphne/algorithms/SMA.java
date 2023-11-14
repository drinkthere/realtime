package capital.daphne.algorithms;

import capital.daphne.AppConfigManager;
import capital.daphne.models.ActionInfo;
import capital.daphne.models.Signal;
import capital.daphne.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.time.Duration;
import java.time.LocalDateTime;

public class SMA implements AlgorithmProcessor {
    private static final Logger logger = LoggerFactory.getLogger(SMA.class);

    private AppConfigManager.AppConfig.AlgorithmConfig ac;

    private LocalDateTime resetDatetime;

    private String benchmarkColumnName;

    public SMA(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
        resetDatetime = null;
    }

    @Override
    public Signal getSignal(Table inputDf, int position, int maxPosition) {
        try {
            // 预处理dataframe，准备好对应的数据字段
            Table df = preProcess(inputDf);

            // 处理数据，获取并返回信号
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
        DoubleColumn vwap = df.doubleColumn("prev_vwap");
        DoubleColumn sma = vwap.rolling(numStatsBars).mean();
        sma.setName(benchmarkColumnName);
        df.addColumns(sma);
        return df;
    }

    private Signal processToGetSignal(Row row, int position, int maxPosition) {
        double volatility = row.getDouble("volatility");

        double volatilityMultiplier = Utils.calToVolatilityMultiplier(ac, volatility);
        LocalDateTime lastBuyDateTime = null;
        LocalDateTime lastSellDateTime = null;

        String redisKey = String.format("%s:%s:%s:%s:LAST_ACTION", ac.getAccountId(), ac.getSymbol(), ac.getSecType(), benchmarkColumnName);
        ActionInfo lastActionInfo = Utils.getLastActionInfo(redisKey);

        Signal.TradeActionType lastAction = lastActionInfo.getAction();
        if (lastAction.equals(Signal.TradeActionType.BUY)) {
            lastBuyDateTime = lastActionInfo.getDateTime();
        } else if (lastAction.equals(Signal.TradeActionType.SELL)) {
            lastSellDateTime = lastActionInfo.getDateTime();
        }

        LocalDateTime datetime = Utils.genUsDateTime(row.getString("date_us"), "yyyy-MM-dd HH:mm:ssXXX");

        double[] signalMargins = Utils.calculateSignalMargin(ac.getSecType(), ac.getSignalMargin(), ac.getPositionSignalMarginOffset(), volatilityMultiplier, position);
        double buySignalMargin = signalMargins[0];
        double sellSignalMargin = signalMargins[1];

        double vwap = row.getDouble("vwap");
        double sma = row.getDouble(benchmarkColumnName);

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
