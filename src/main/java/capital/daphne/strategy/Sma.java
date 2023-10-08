package capital.daphne.strategy;

import capital.daphne.AppConfig;
import capital.daphne.datasource.ibkr.Ibkr;
import capital.daphne.utils.Utils;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class Sma implements Strategy {
    private static final Logger logger = LoggerFactory.getLogger(Ibkr.class);

    private AppConfig config;

    private Map<String, Map<String, Object>> symbolActionMap;

    private Map<String, Integer> numStatsBarsMap;

    public Sma(AppConfig conf) {
        config = conf;

        Map<String, Object> actionMap = new HashMap<>();
        actionMap.put("lastAction", TradeActionType.NO_ACTION);
        actionMap.put("lastBuyDatetime", null);
        actionMap.put("lastSellDatetime", null);
        symbolActionMap = new HashMap<>();
        numStatsBarsMap = new HashMap<>();
        for (AppConfig.SymbolConfig sc : conf.getSymbols()) {
            String key = Utils.genKey(sc.getSymbol(), sc.getSecType());
            symbolActionMap.put(key, actionMap);
            numStatsBarsMap.put(key, sc.getStrategy().getNumStatsBars());
        }
    }

    @Override
    public TradeActionType getSignalSide(String symbol, String secType, Table inputDf, int position) {
        String key = Utils.genKey(symbol, secType);
        // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
        int numStatsBars = numStatsBarsMap.get(key);
        String benchmark = "sma" + numStatsBars;
        Table df = addBenchMarkColumn(inputDf, benchmark, numStatsBars);
        Row latestBar = df.row(df.rowCount() - 1);

        // 获取symbol的相关配置
        AppConfig.SymbolConfig sc;
        Optional<AppConfig.SymbolConfig> symbolItemOptional = config.getSymbols().stream()
                .filter(item -> item.getSymbol().equals(symbol) && item.getSecType().equals(secType))
                .findFirst();
        if (symbolItemOptional.isPresent()) {
            sc = symbolItemOptional.get();
        } else {
            logger.error("Can't find the configuration of symbol=" + symbol + ", secType=" + secType);
            return TradeActionType.NO_ACTION;
        }

        double volatility = latestBar.getDouble("volatility");
        double volatilityMultiplier = calToVolatilityMultiplier(sc, volatility);

        TradeActionType action = processPriceBar(sc, latestBar, volatilityMultiplier, position, benchmark);

        return action;
    }

    private Table addBenchMarkColumn(Table df, String benchmark, int numStatsBars) {
        DoubleColumn vwap = df.doubleColumn("prev_vwap");
        DoubleColumn sma = vwap.rolling(numStatsBars).mean();
        sma.setName(benchmark);
        df.addColumns(sma);
        return df;
    }

    private double calToVolatilityMultiplier(AppConfig.SymbolConfig sc, double volatility) {
        return 1 + sc.getStrategy().getVolatilityA() +
                sc.getStrategy().getVolatilityB() * volatility +
                sc.getStrategy().getVolatilityC() * volatility * volatility;
    }

    synchronized private TradeActionType processPriceBar(AppConfig.SymbolConfig sc, Row row, double volatilityMultiplier, int position, String benchmarkColumn) {
        String symbol = sc.getSymbol();
        String secType = sc.getSecType();
        String key = Utils.genKey(symbol, secType);

        Map<String, Object> actionMap = symbolActionMap.get(key);
        TradeActionType lastAction = (TradeActionType) actionMap.get("lastAction");
        LocalDateTime lastBuyDatetime = (LocalDateTime) actionMap.get("lastBuyDatetime");
        LocalDateTime lastSellDatetime = (LocalDateTime) actionMap.get("lastSellDatetime");

        TradeActionType action = TradeActionType.NO_ACTION;
        LocalTime portfolioCloseTime = LocalTime.of(15, 50, 0);
        LocalTime marketOpenTime = LocalTime.of(9, 30, 0);
        LocalTime marketCloseTime = LocalTime.of(16, 0, 0);

        LocalTime tradeEndTime = !sc.getStrategy().isPortfolioRequiredToClose() ? marketCloseTime : portfolioCloseTime;

        String date_us = row.getString("date_us");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
        LocalDateTime datetime = zonedDateTime.toLocalDateTime();
        LocalTime time = datetime.toLocalTime();


        // System.out.println(portfolioPositions);
        double[] signalMargins = calculateSignalMargin(sc, volatilityMultiplier, position);
        double buySignalMargin = signalMargins[0];
        double sellSignalMargin = signalMargins[1];

        if ((time.isAfter(marketOpenTime) && time.isBefore(tradeEndTime)) || time.equals(marketOpenTime) || time.equals(tradeEndTime) || secType.equals("FUT")) {
            double vwap = row.getDouble("vwap");

            double sma = row.getDouble(benchmarkColumn);
            long buyIntervalSeconds = 0L;
            if (!lastAction.equals(TradeActionType.NO_ACTION) && lastBuyDatetime != null) {
                Duration buyDuration = Duration.between(lastBuyDatetime, datetime);
                buyIntervalSeconds = buyDuration.getSeconds();
            }
            long sellIntervalSeconds = 0L;
            if (!lastAction.equals(TradeActionType.NO_ACTION) && lastSellDatetime != null) {
                Duration sellDuration = Duration.between(lastSellDatetime, datetime);
                sellIntervalSeconds = sellDuration.getSeconds();
            }
            logger.info(String.format("symbol=%s, secType=%s, vwap=%f, sma=%f, vwap<=sma*(1-buySignalMargin):%s, vwap >= sma * (1 + sellSignalMargin)：%s", symbol, secType, vwap, sma, vwap <= sma * (1 - buySignalMargin), vwap >= sma * (1 + sellSignalMargin)));
            if (vwap <= sma * (1 - buySignalMargin)
                    && (lastAction.equals(TradeActionType.NO_ACTION) || lastAction.equals(TradeActionType.SELL) || buyIntervalSeconds >= sc.getStrategy().getMinIntervalBetweenSignal())
                    && (position < sc.getStrategy().getMaxPortfolioPositions() || !sc.getStrategy().isHardLimit())) {
                action = TradeActionType.BUY;
                lastAction = TradeActionType.BUY;
                lastBuyDatetime = datetime;

            } else if (vwap >= sma * (1 + sellSignalMargin)
                    && (lastAction.equals(TradeActionType.NO_ACTION) || lastAction.equals(TradeActionType.BUY) || sellIntervalSeconds >= sc.getStrategy().getMinIntervalBetweenSignal())
                    && (position > (-sc.getStrategy().getMaxPortfolioPositions()) || !sc.getStrategy().isHardLimit())) {
                action = TradeActionType.SELL;
                lastAction = TradeActionType.SELL;
                lastSellDatetime = datetime;
            }
        } else {
            lastAction = TradeActionType.NO_ACTION;
            lastBuyDatetime = null;
            lastSellDatetime = null;
        }

        actionMap.put("lastAction", lastAction);
        actionMap.put("lastBuyDatetime", lastBuyDatetime);
        actionMap.put("lastSellDatetime", lastSellDatetime);
        symbolActionMap.put(key, actionMap);

        return action;
    }

    private double[] calculateSignalMargin(AppConfig.SymbolConfig sc, double volatilityMultiplier, int position) {
        float signalMargin = sc.getStrategy().getSignalMargin();
        int maxPortfolioPositions = sc.getStrategy().getMaxPortfolioPositions();
        float positionSignalMarginOffset = sc.getStrategy().getPositionSignalMarginOffset();

        float positionOffset = position / maxPortfolioPositions;

        double buySignalMargin = (signalMargin + positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        double sellSignalMargin = (signalMargin - positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        return new double[]{buySignalMargin, sellSignalMargin};
    }
}
