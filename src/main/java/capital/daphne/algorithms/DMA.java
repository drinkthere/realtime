package capital.daphne.algorithms;

import capital.daphne.AppConfigManager;
import capital.daphne.models.ActionInfo;
import capital.daphne.models.Signal;
import capital.daphne.services.ta.TA;
import capital.daphne.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.lang.reflect.Constructor;
import java.time.Duration;
import java.time.LocalDateTime;

public class DMA implements AlgorithmProcessor {
    private static final Logger logger = LoggerFactory.getLogger(DMA.class);

    private AppConfigManager.AppConfig.AlgorithmConfig ac;

    private AppConfigManager.AppConfig.DMAParams dp;
    private String benchmarkColumnName;

    public DMA(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
        dp = algorithmConfig.getDmaParams();
        benchmarkColumnName = "dma";
    }

    @Override
    public Signal getSignal(Table inputDf, int position, int maxPosition, double bidPrice, double askPrice) {
        try {
            // 预处理dataframe，准备好对应的数据字段
            Table taDf = preProcess(inputDf);

            // 处理数据，获取并返回信号
            Row latestRow = inputDf.row(inputDf.rowCount() - 1);
            Row latestTaRow = taDf.row(taDf.rowCount() - 1);
            return processToGetSignal(latestRow, latestTaRow, position, maxPosition);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("save signal failed, error:" + e.getMessage());
            return null;
        }
    }

    private Table preProcess(Table df) {
        Table taDf = Table.create("TA");

        String longFilterKey = dp.getLongFilterKey();
        String shortFilterKey = dp.getShortFilterKey();

        if (!longFilterKey.equals("-")) {
            TA longTa = null;
            try {
                String packageToClassName = "capital.daphne.services.ta." + longFilterKey;
                // 使用反射加载类
                Class<?> clazz = Class.forName(packageToClassName);
                Constructor<?> constructor = clazz.getConstructor();
                longTa = (TA) constructor.newInstance();
                df = longTa.ta(df);
                taDf.addColumns(df.doubleColumn(longFilterKey));
            } catch (ClassNotFoundException e) {
                logger.error("Class not found: " + longFilterKey);
                System.exit(1);
            } catch (Exception e) {
                logger.error("Error creating instance for: " + longFilterKey);
                e.printStackTrace();
                System.exit(1);
            }
        }

        if (!shortFilterKey.equals("-")) {
            // todo generate short filter column
        }

        int fastWindow = dp.getFastWindow();
        int slowWindow = dp.getSlowWindow();
        int trendWindow = dp.getTrendWindow() + slowWindow;

        DoubleColumn vwap = df.doubleColumn("vwap");
        DoubleColumn fast = vwap.rolling(fastWindow).mean();
        DoubleColumn slow = vwap.rolling(slowWindow).mean();
        fast.setName("fast");
        slow.setName("slow");

        int delayBars = dp.getDelayOpenSeconds() / dp.getBarSeconds();
        DoubleColumn fast1 = fast.lag(1 + delayBars).setName("fast1");
        DoubleColumn fast2 = fast.lag(2 + delayBars).setName("fast2");
        DoubleColumn slow1 = slow.lag(1 + delayBars).setName("slow1");
        DoubleColumn slow2 = slow.lag(2 + delayBars).setName("slow2");

        DoubleColumn fast1Cur = fast.lag(1).setName("fast1Cur");
        DoubleColumn slow1Cur = slow.lag(1).setName("slow1Cur");
        DoubleColumn fast2Cur = fast.lag(2).setName("fast2Cur");
        DoubleColumn slow2Cur = slow.lag(2).setName("slow2Cur");


        taDf.addColumns(fast1);
        taDf.addColumns(fast2);
        taDf.addColumns(slow1);
        taDf.addColumns(slow2);

        taDf.addColumns(fast1Cur);
        taDf.addColumns(slow1Cur);
        taDf.addColumns(fast2Cur);
        taDf.addColumns(slow2Cur);

        if (trendWindow > slowWindow) {
            DoubleColumn trend = vwap.rolling(trendWindow).mean();
            trend.setName("trend");
            DoubleColumn trend1 = trend.lag(1 + delayBars).setName("trend1");
            DoubleColumn trend1Cur = trend.lag(1).setName("trend1Cur");
            taDf.addColumns(trend1);
            taDf.addColumns(trend1Cur);
        }
        return taDf;
    }

    private Signal processToGetSignal(Row row, Row taRow, int position, int maxPosition) {
        boolean onlyLong = isOnlyLong();
        boolean onlyShort = isOnlyShort();
        if (isBothOrderMode()) {
            onlyLong = true;
            onlyShort = true;
        }

        double fast1 = taRow.getDouble("fast1");
        double slow1 = taRow.getDouble("slow1");
        double fast2 = taRow.getDouble("fast2");
        double slow2 = taRow.getDouble("slow2");
        double fast1Cur = taRow.getDouble("fast1Cur");
        double slow1Cur = taRow.getDouble("slow1Cur");
        double fast2Cur = taRow.getDouble("fast2Cur");
        double slow2Cur = taRow.getDouble("slow2Cur");

        // using ta filter
        String longFilterKey = dp.getLongFilterKey();
        String shortFilterKey = dp.getShortFilterKey();
        double longFilterGt = dp.getLongFilterGt();
        double shortFilterGt = dp.getShortFilterGt();

        // handle trend filter
        boolean trendFilterLong = true;
        boolean trendFilterShort = true;
        boolean trendFilterLongCur = true;
        boolean trendFilterShortCur = true;
        int tw = dp.getTrendWindow();
        if (tw != 0) {
            // need use trend as filter
            double trend1 = taRow.getDouble("trend1");
            double trend1Cur = taRow.getDouble("trend1Cur");
            trendFilterLong = slow1 > trend1;
            trendFilterShort = slow1 < trend1;
            trendFilterLongCur = slow1Cur > trend1Cur;
            trendFilterShortCur = slow1Cur < trend1Cur;
        }

        boolean goldCross = fast1 > slow1 && fast2 < slow2;
        boolean deathCross = fast1 < slow1 && fast2 > slow2;
        boolean goldCrossCur = fast1Cur > slow1Cur && fast2Cur < slow2Cur;
        boolean deathCrossCur = fast1Cur < slow1Cur && fast2Cur > slow2Cur;
        boolean stayGold = fast1Cur > slow1Cur && fast1Cur >= fast1;
        boolean stayDeath = fast1Cur < slow1Cur && fast1Cur <= fast1;
        // logger.info(String.format("gc=%b|dc=%b|f1=%f|s1=%f|f2=%f|s2=%f", goldCrossCur, deathCrossCur, fast1, slow1, fast2, slow2));

//        handle filter
        boolean longFilter = true;
        boolean shortFilter = true;
        if (!longFilterKey.equals("-")) {
            double ta1 = taRow.getDouble(longFilterKey);
            longFilter = ta1 >= longFilterGt;
        }
        if (!shortFilterKey.equals("-")) {
            double ta1 = taRow.getDouble(shortFilterKey);
            shortFilter = ta1 >= shortFilterGt;
        }

        LocalDateTime datetime = Utils.genUsDateTime(row.getString("date_us"), "yyyy-MM-dd HH:mm:ssXXX");

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

        double vwap = row.getDouble("vwap");
        Signal signal = null;
        if (onlyLong) {
            logger.info(String.format("%b|%b|%b|%b|%b",
                    goldCross, trendFilterLong, longFilter, stayGold, trendFilterLongCur));
            if ((lastAction.equals(Signal.TradeActionType.NO_ACTION) || buyIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                    && position < maxPosition && goldCross && trendFilterLong && longFilter && stayGold && trendFilterLongCur) {
                signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, ac.getOrderSize(), Signal.OrderType.OPEN, benchmarkColumnName);
            } else if (position > 0 && deathCrossCur) {
                signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, -position, Signal.OrderType.CLOSE, benchmarkColumnName);
            }

            if (signal != null) {
                return signal;
            }
        }

        if (onlyShort) {
            logger.info(String.format("%b|%b|%b|%b|%b",
                    deathCross, trendFilterShort, shortFilter, stayDeath, trendFilterShortCur));
            if ((lastAction.equals(Signal.TradeActionType.NO_ACTION) || sellIntervalSeconds >= ac.getMinIntervalBetweenSignal())
                    && (position > -maxPosition) && deathCross && trendFilterShort && shortFilter && stayDeath && trendFilterShortCur) {
                signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, -ac.getOrderSize(), Signal.OrderType.OPEN, benchmarkColumnName);
            } else if (position < 0 && goldCrossCur) {
                signal = Utils.fulfillSignal(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), vwap, -position, Signal.OrderType.CLOSE, benchmarkColumnName);
            }
        }
        return signal;
    }

    private boolean isOnlyLong() {
        return dp.getOrderMode().equals("l");
    }

    public boolean isOnlyShort() {
        return dp.getOrderMode().equals("s");
    }

    public boolean isBothOrderMode() {
        return dp.getOrderMode().equals("b");
    }
}
