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
        for (AppConfig.SymbolConfig sc : conf.getSymbols()) {
            symbolActionMap.put(sc.getSymbol(), actionMap);
            numStatsBarsMap.put(sc.getSymbol(), sc.getNumStatsBars());
        }
    }

    @Override
    public TradeActionType getSignalSide(String symbol, Table inputDf, int position) {
        // 生成关键指标，这里是sma+numStatsBars,e.g. sma12
        int numStatsBars = numStatsBarsMap.get(symbol);
        String benchmark = "sma" + numStatsBars;
        Table df = addBenchMarkColumn(inputDf, benchmark, numStatsBars);
        Row latestBar = df.row(df.rowCount() - 1);

        // 获取symbol的相关配置
        AppConfig.SymbolConfig symbolConfig;
        Optional<AppConfig.SymbolConfig> symbolItemOptional = config.getSymbols().stream()
                .filter(item -> item.getSymbol().equals(symbol))
                .findFirst();
        if (symbolItemOptional.isPresent()) {
            symbolConfig = symbolItemOptional.get();
        } else {
            logger.error("Can't find the configuration of symbol=" + symbol);
            return TradeActionType.NO_ACTION;
        }

        double volatility = latestBar.getDouble("volatility");
        double volatilityMultiplier = calToVolatilityMultiplier(symbolConfig, volatility);

        TradeActionType action = processPriceBar(symbolConfig, latestBar, volatilityMultiplier, position, benchmark);

        return action;
    }

    private Table addBenchMarkColumn(Table df, String benchmark, int numStatsBars) {
        DoubleColumn vwap = df.doubleColumn("prev_vwap");
        DoubleColumn sma = vwap.rolling(numStatsBars).mean();
        sma.setName(benchmark);
        df.addColumns(sma);
        return df;
    }

    private double calToVolatilityMultiplier(AppConfig.SymbolConfig symbolConfig, double volatility) {
        return 1 + symbolConfig.getVolatilityA() + symbolConfig.getVolatilityB() * volatility + symbolConfig.getVolatilityC() * volatility * volatility;
    }

    private TradeActionType processPriceBar(AppConfig.SymbolConfig symbolConfig, Row row, double volatilityMultiplier, int position, String benchmarkColumn) {
        String symbol = symbolConfig.getSymbol();
        Map<String, Object> actionMap = symbolActionMap.get(symbol);
        TradeActionType lastAction = (TradeActionType) actionMap.get("lastAction");
        LocalDateTime lastBuyDatetime = (LocalDateTime) actionMap.get("lastBuyDatetime");
        LocalDateTime lastSellDatetime = (LocalDateTime) actionMap.get("lastSellDatetime");


        TradeActionType action = TradeActionType.NO_ACTION;
        LocalTime portfolioCloseTime = LocalTime.of(15, 50, 0);
        LocalTime marketOpenTime = LocalTime.of(9, 30, 0);
        LocalTime marketCloseTime = LocalTime.of(16, 0, 0);

        LocalTime tradeEndTime = !symbolConfig.isPortfolioRequiredToClose() ? marketCloseTime : portfolioCloseTime;

        String date_us = row.getString("date_us");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
        LocalDateTime datetime = zonedDateTime.toLocalDateTime();
        LocalTime time = datetime.toLocalTime();


        // System.out.println(portfolioPositions);
        double[] signalMargins = calculateSignalMargin(symbolConfig, volatilityMultiplier, position);
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
                    && (lastAction == TradeActionType.NO_ACTION || lastAction.equals(TradeActionType.SELL) || buyIntervalSeconds >= symbolConfig.getMinIntervalBetweenSignal())
                    && ((position < symbolConfig.getMaxPortfolioPositions() && symbolConfig.isHardLimit()) || !symbolConfig.isHardLimit())) {
                action = TradeActionType.BUY;
                lastAction = TradeActionType.BUY;
                lastBuyDatetime = datetime;

            } else if (vwap >= sma12 * (1 + sellSignalMargin)
                    && (lastAction == TradeActionType.NO_ACTION || lastAction.equals(TradeActionType.BUY) || sellIntervalSeconds >= symbolConfig.getMinIntervalBetweenSignal())
                    && ((position > (-symbolConfig.getMaxPortfolioPositions()) && symbolConfig.isHardLimit()) || symbolConfig.isHardLimit())) {
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
        symbolActionMap.put(symbol, actionMap);
        return action;
    }

    public double[] calculateSignalMargin(AppConfig.SymbolConfig symbolConf, double volatilityMultiplier, int position) {
        float signalMargin = symbolConf.getSignalMargin();
        int maxPortfolioPositions = symbolConf.getMaxPortfolioPositions();
        float positionSignalMarginOffset = symbolConf.getPositionSignalMarginOffset();

        float positionOffset = position / maxPortfolioPositions;

        double buySignalMargin = (signalMargin + positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        double sellSignalMargin = (signalMargin - positionSignalMarginOffset * positionOffset) * volatilityMultiplier;
        return new double[]{buySignalMargin, sellSignalMargin};
    }
}
