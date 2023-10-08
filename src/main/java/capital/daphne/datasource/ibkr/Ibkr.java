package capital.daphne.datasource.ibkr;

import capital.daphne.AppConfig;
import capital.daphne.Db;
import capital.daphne.datasource.Signal;
import capital.daphne.strategy.Sma;
import capital.daphne.strategy.Strategy;
import capital.daphne.utils.Utils;
import com.ib.client.Contract;
import com.ib.client.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.*;


public class Ibkr {
    private static final Logger logger = LoggerFactory.getLogger(Ibkr.class);

    private IbkrController ic;

    private AppConfig config;

    private Db db;
    private TopMktDataHandler topMktDataHandler;

    private RealTimeBarHandler realTimeBarHandler;

    private PositionHandler positionHandler;

    private Map<String, Strategy> strategyHandlerMap;

    public Ibkr(AppConfig appConfig, Db dbHandler) {
        ConnectHandler connectHandler = new ConnectHandler();
        // ic = new IbkrController(connectHandler, new TwsLogger(), new TwsLogger());
        ic = new IbkrController(connectHandler);
        config = appConfig;
        db = dbHandler;
        topMktDataHandler = new TopMktDataHandler(config.getSymbols());
        positionHandler = new PositionHandler();
        realTimeBarHandler = new RealTimeBarHandler(db, config.getSymbols(), topMktDataHandler);

        strategyHandlerMap = new HashMap<>();
        for (AppConfig.SymbolConfig sc : config.getSymbols()) {
            AppConfig.Strategy strategy = sc.getStrategy();
            if (strategy != null) {
                Strategy strategyHandler = initStrategyHandler(strategy.getName());
                strategyHandlerMap.put(Utils.genKey(sc.getSymbol(), sc.getSecType()), strategyHandler);
            }
        }
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

    public void initWap(Map<String, double[]> symbolWapMap) {
        realTimeBarHandler.initWap(symbolWapMap);
    }


    public void startRealtimeWatcher() {
        logger.info("subscribe 5s bar");
        sub5sBars();

        logger.info("subscribe ticker");
        subMktData();

        logger.info("subscribe position");
        subPosition();
    }

    public List<Signal> getTradeSignals() {
        List<Signal> signalList = new ArrayList<>();

        for (AppConfig.SymbolConfig sc : config.getSymbols()) {
            String symbol = sc.getSymbol();
            String secType = sc.getSecType();
            if ((secType.equals("STK") && Utils.isMarketOpen()) || secType.equals("FUT")) {
                Signal signal = new Signal();
                signal.setValid(false);

                // 获取position信息
                int position = positionHandler.getSymbolPosition(symbol, secType);
                // 获取5s bar信息
                Table df = realTimeBarHandler.getDataTable(symbol, secType);
                if (df == null) {
                    logger.info("symbol=" + symbol + ", secType= " + secType + ", dataframe is not ready");
                    continue;
                }

                // 获取当前bid和ask信息
                double bidPrice = topMktDataHandler.getBidPrice(symbol, secType);
                double askPrice = topMktDataHandler.getAskPrice(symbol, secType);
                logger.debug(bidPrice + " " + askPrice);
                // 判断是否要下单
                Strategy strategyHandler = strategyHandlerMap.get(Utils.genKey(symbol, secType));
                Strategy.TradeActionType side = strategyHandler.getSignalSide(symbol, secType, df, position);
                if (side.equals(Strategy.TradeActionType.NO_ACTION)) {
                    logger.info("symbol=" + symbol + ", secType= " + secType + ", no action signal");
                    continue;
                }

                Row latestBar = df.row(df.rowCount() - 1);
                signal.setValid(true);
                signal.setSymbol(symbol);
                signal.setSecType(secType);
                signal.setSide(side);
                signal.setBidPrice(bidPrice);
                signal.setAskPrice(askPrice);
                signal.setWap(latestBar.getDouble("vwap"));
                signal.setQuantity(sc.getStrategy().getOrderSize());
                signal.setRewrite(sc.getRewrite());
                signal.setParallel(sc.getParallel());
                signalList.add(signal);
            } else {
                logger.info("market is not open");
            }
        }
        return signalList;
    }

    private Contract genContract(String symbol, String secType) {
        AppConfig.SymbolConfig sc;
        Optional<AppConfig.SymbolConfig> symbolItemOptional = config.getSymbols().stream()
                .filter(item -> item.getSymbol().equals(symbol) && item.getSecType().equals(secType))
                .findFirst();
        if (symbolItemOptional.isPresent()) {
            sc = symbolItemOptional.get();
        } else {
            logger.error("Can't find the configuration of symbol=" + symbol + ", secType=" + secType);
            return null;
        }

        Contract contract = new Contract();
        contract.symbol(symbol); // 设置合约标的
        if (secType.equals("FUT")) {
            contract.secType(Types.SecType.FUT);
            contract.lastTradeDateOrContractMonth(sc.getLastTradeDateOrContractMonth());
            contract.multiplier(sc.getMultiplier());
        } else if (secType.equals("STK")) {
            contract.secType(Types.SecType.STK);
        } else if (secType.equals("CFD")) {
            contract.secType(Types.SecType.CFD);
        }
        contract.exchange(sc.getExchange()); // 设置交易所
        contract.primaryExch(sc.getPrimaryExchange());
        contract.currency(sc.getCurrency()); // 设置货币
        return contract;
    }

    private Strategy initStrategyHandler(String strategyStr) {
        switch (strategyStr) {
            case "SMA":
            default:
                return new Sma(config);
        }
    }

    private void sub5sBars() {
        try {
            Map<String, Integer> barSblReqIdMap = new HashMap<>();
            for (AppConfig.SymbolConfig symbolConfig : config.getSymbols()) {
                String symbol = symbolConfig.getSymbol();
                String secType = symbolConfig.getSecType();
                Contract contract = genContract(symbol, secType);
                ic.reqContractDetails(contract, new ContractDetailsHandler());
                boolean rthOnly = false;
                int reqId = ic.reqRealTimeBars(contract, Types.WhatToShow.TRADES, rthOnly, realTimeBarHandler);
                barSblReqIdMap.put(Utils.genKey(symbol, secType), reqId);
                realTimeBarHandler.setSblReqIdMap(barSblReqIdMap);
            }
        } catch (Exception e) {
            logger.error("failed to subscribe 5s bar info, err:" + e.getMessage());
        }
    }

    private void subMktData() {
        try {
            for (AppConfig.SymbolConfig sc : config.getSymbols()) {
                String symbol = sc.getSymbol();
                String secType = sc.getSecType();
                Contract contract = genContract(symbol, secType);
                // contract, genericTickList, snapshot, regulatorySnapshot, ITopMktDataHandler
                int reqId = ic.reqTopMktData(contract, "", false, false, topMktDataHandler);
                topMktDataHandler.bindReqIdSymbol(symbol, secType, reqId);
            }
        } catch (Exception e) {
            logger.error("failed to subscribe market ticker info, err:" + e.getMessage());
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
