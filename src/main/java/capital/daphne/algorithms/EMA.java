package capital.daphne.algorithms;

import capital.daphne.AppConfigManager;
import capital.daphne.models.ActionInfo;
import capital.daphne.models.Signal;
import capital.daphne.services.BarSvc;
import capital.daphne.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    public Signal getSignal(Table df, int position, int maxPosition, double bidPrice, double askPrice) {
        try {
            // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
            int numStatsBars = ac.getNumStatsBars();
            benchmarkColumnName = ac.getName() + numStatsBars;

            String key = ac.getAccountId() + ":" + ac.getSymbol() + ":" + ac.getSecType();

            // 获取ema
            Double ema = barSvc.getEma(key);
            if (ema == 0) {
                logger.warn(key + " ema is not ready");
                return null;
            }

            // 获取volatility
            Double[] volaArr = barSvc.getVolatilityInRedis(key);
            if (volaArr == null) {
                return null;
            }
            double volatility = volaArr[0];
            double volatilityMultiplier = volaArr[1];

            Row row = df.row(df.rowCount() - 1);
            return processToGetSignal(row, position, maxPosition, ema, volatility, volatilityMultiplier, bidPrice, askPrice);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("save signal failed, error:" + e.getMessage());
            return null;
        }
    }


    private Signal processToGetSignal(Row row, int position, int maxPosition, double ema, double volatility, double volatilityMultiplier, double bidPrice, double askPrice) {
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

        Signal signal = null;
        logger.info(String.format("%s|%s|%s|%s|place|vol=%f|volMulti:%f|ema=%f|sm=%f|bsm=%f|ssm=%f|vwap=%f|ask=%f|<=%f %s|bid=%f|>=%f %s|pos=%d|%s",
                time, ac.getAccountId(), ac.getSymbol(), ac.getSecType(), volatility, volatilityMultiplier, ema, ac.getSignalMargin(),
                buySignalMargin, sellSignalMargin, vwap, askPrice, longThreshold, askPrice <= longThreshold, bidPrice, shortThreshold, bidPrice >= shortThreshold, position, lastAction));
        if (askPrice <= longThreshold
                && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.SELL) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                && (position < maxPosition) && resetDatetime == null) {
            signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, ac.getOrderSize(), Signal.OrderType.OPEN, benchmarkColumnName);
        } else if (bidPrice >= shortThreshold
                && (lastAction.equals(Signal.TradeActionType.NO_ACTION) || lastAction.equals(Signal.TradeActionType.BUY) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                && (position > -maxPosition) && resetDatetime == null) {
            signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, -ac.getOrderSize(), Signal.OrderType.OPEN, benchmarkColumnName);
        }
        if (signal != null && ac.getGtdCancelAfterSec() > 0) {
            // 如果是挂gtd订单，这里设置有效挂单时间
            signal.setGtd(true);
            signal.setGtdSec(ac.getGtdCancelAfterSec());
        }
        return signal;
    }
}
