package capital.daphne.datasource.ibkr;

import capital.daphne.AppConfig;
import capital.daphne.Db;
import capital.daphne.datasource.Signal;
import capital.daphne.service.BarSvc;
import capital.daphne.service.TickerSvc;
import capital.daphne.strategy.Sma;
import capital.daphne.strategy.Strategy;
import capital.daphne.utils.Utils;
import com.ib.client.TickType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.*;


public class Ibkr {
    private static final Logger logger = LoggerFactory.getLogger(Ibkr.class);

    private final IbkrController ic;

    private final AppConfig config;
    private final TickerSvc tickerService;
    private final BarSvc barService;
    private final PositionHandler positionHandler;
    private Map<String, Strategy> strategyHandlerMap;

    public Ibkr(AppConfig appConfig, Db db) {
        ConnectHandler connectHandler = new ConnectHandler();
        ic = new IbkrController(connectHandler);
        config = appConfig;
        positionHandler = new PositionHandler();
        tickerService = new TickerSvc();
        barService = new BarSvc();

        // 初始化symbol对应的strategy handlers
        initSymbolStrategyHandlers();
    }

    public void connectTWS() {
        // 最后一个参数是connectionOpts，sdk中没有用到
        ic.connect(config.getTws().getHost(), config.getTws().getPort(), config.getTws().getClientId(), null);
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            logger.error(e.getMessage());
        }
    }

    public void startTwsWatcher() {
        logger.info("subscribe position");
        subPosition();
    }

    public List<Signal> getTradeSignals() {
        List<Signal> signalList = new ArrayList<>();

        for (AppConfig.SymbolConfig sc : config.getSymbols()) {
            String symbol = sc.getSymbol();
            String secType = sc.getSecType();
            String key = Utils.genKey(symbol, secType);
            Signal tradeSignal = getTradeSignal(key);
            signalList.add(tradeSignal);
        }
        return signalList;
    }

    public Signal getTradeSignal(String msg) {
        Signal signal = new Signal();
        signal.setValid(false);

        // 更新当前交易日的max_wap和min_wap
        String[] splitArr = msg.split("\\.");
        String symbol = splitArr[0];
        String secType = splitArr[1];
        AppConfig.SymbolConfig sc;
        Optional<AppConfig.SymbolConfig> symbolItemOptional = config.getSymbols().stream()
                .filter(item -> item.getSymbol().equals(symbol) && item.getSecType().equals(secType))
                .findFirst();
        if (symbolItemOptional.isPresent()) {
            sc = symbolItemOptional.get();
        } else {
            logger.error("Can't find the configuration of symbol=" + symbol + ", secType=" + secType);
            return signal;
        }

        String key = Utils.genKey(symbol, secType);
        // FUT全天交易，STK和CDF在正常交易区间交易
        if (secType.equals("FUT") || Utils.isMarketOpen()) {
            // 获取5s bar信息
            Table df = barService.getDataTable(key, sc.getStrategy().getNumStatsBars());
            if (df == null) {
                logger.info(msg + ", dataframe is not ready");
                return signal;
            }

            // 获取position信息
            int[] positionArr = positionHandler.getPosition(sc);
            int position = positionArr[0];
            int maxPosition = positionArr[1];

            // 判断是否要下单
            Strategy strategyHandler = strategyHandlerMap.get(key);
            Strategy.TradeActionType side = strategyHandler.getSignalSide(df, position, maxPosition);
            if (side.equals(Strategy.TradeActionType.NO_ACTION)) {
                logger.info(msg + ", no action signal");
                return signal;
            }

            // 获取当前bid和ask信息
            double bidPrice = tickerService.getPrice(key, TickType.BID);
            double askPrice = tickerService.getPrice(key, TickType.ASK);
            logger.info(bidPrice + " " + askPrice);

            Row latestBar = df.row(df.rowCount() - 1);
            signal.setValid(true);
            signal.setSymbol(symbol);
            signal.setSecType(secType);
            signal.setSide(side);
            signal.setBidPrice(bidPrice);
            signal.setAskPrice(askPrice);
            signal.setWap(latestBar.getDouble("vwap"));
            signal.setQuantity(sc.getStrategy().getOrderSize());
            signal.setSymbolConfig(sc);
        } else {
            logger.info("market is not open");
        }
        return signal;
    }

    private void initSymbolStrategyHandlers() {
        strategyHandlerMap = new HashMap<>();
        for (AppConfig.SymbolConfig sc : config.getSymbols()) {
            Strategy strategyHandler;
            //兼容多策略
            AppConfig.Strategy strategy = sc.getStrategy();
            switch (strategy.getName()) {
                case "SMA":
                default:
                    strategyHandler = new Sma(sc);
                    break;
            }
            strategyHandlerMap.put(Utils.genKey(sc.getSymbol(), sc.getSecType()), strategyHandler);
        }
    }

    private void subPosition() {
        try {
            ic.reqPositions(positionHandler);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("failed to subscribe position, err:" + e.getMessage());
        }
    }
}
