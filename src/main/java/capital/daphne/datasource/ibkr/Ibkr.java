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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Ibkr {
    private static final Logger logger = LoggerFactory.getLogger(Ibkr.class);

    private final IbkrController ic;

    private final AppConfig config;
    private final TopMktDataHandler topMktDataHandler;
    private final RealTimeBarHandler realTimeBarHandler;
    private final PositionHandler positionHandler;
    private Map<String, Strategy> strategyHandlerMap;

    public Ibkr(AppConfig appConfig, Db db) {
        ConnectHandler connectHandler = new ConnectHandler();
        // ic = new IbkrController(connectHandler, new TwsLogger(), new TwsLogger());
        ic = new IbkrController(connectHandler);
        config = appConfig;
        topMktDataHandler = new TopMktDataHandler(config.getSymbols());
        positionHandler = new PositionHandler();
        realTimeBarHandler = new RealTimeBarHandler(db, config.getSymbols());

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

    // 初始化wap信息，用于计算波动率，因为要保存前一天的数据，所以用数据库做了缓存
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
            String key = Utils.genKey(symbol, secType);
            if ((secType.equals("STK") && Utils.isMarketOpen()) || secType.equals("FUT")) {
                Signal signal = new Signal();
                signal.setValid(false);

                // 获取5s bar信息
                Table df = realTimeBarHandler.getDataTable(key);
                if (df == null) {
                    logger.info("symbol=" + symbol + ", secType= " + secType + ", dataframe is not ready");
                    continue;
                }

                // 获取当前bid和ask信息
                double bidPrice = topMktDataHandler.getBidPrice(key);
                double askPrice = topMktDataHandler.getAskPrice(key);
                logger.debug(bidPrice + " " + askPrice);

                // 获取position信息
                int[] positionArr = positionHandler.getSymbolPosition(sc);
                int position = positionArr[0];
                int maxPosition = positionArr[1];

                // 判断是否要下单
                Strategy strategyHandler = strategyHandlerMap.get(key);
                Strategy.TradeActionType side = strategyHandler.getSignalSide(df, position, maxPosition);
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
                signal.setSymbolConfig(sc);
                signalList.add(signal);
            } else {
                logger.info("market is not open");
            }
        }
        return signalList;
    }

    private void initSymbolStrategyHandlers() {
        strategyHandlerMap = new HashMap<>();
        for (AppConfig.SymbolConfig sc : config.getSymbols()) {
            Strategy strategyHandler;
            strategyHandler = new Sma(sc);

/*
            //兼容多策略
            AppConfig.Strategy strategy = sc.getStrategy();
            switch (strategy.getName()) {
                case "SMA":
                default:
                    strategyHandler = new Sma(sc);
                    break;
            }

 */
            strategyHandlerMap.put(Utils.genKey(sc.getSymbol(), sc.getSecType()), strategyHandler);
        }
    }


    private Contract genContract(AppConfig.SymbolConfig sc) {
        Contract contract = new Contract();
        contract.symbol(sc.getSymbol()); // 设置合约标的
        switch (sc.getSecType()) {
            case "FUT" -> {
                contract.secType(Types.SecType.FUT);
                contract.lastTradeDateOrContractMonth(sc.getLastTradeDateOrContractMonth());
                contract.multiplier(sc.getMultiplier());
            }
            case "STK" -> contract.secType(Types.SecType.STK);
            case "CFD" -> contract.secType(Types.SecType.CFD);
        }
        contract.exchange(sc.getExchange()); // 设置交易所
        contract.primaryExch(sc.getPrimaryExchange());
        contract.currency(sc.getCurrency()); // 设置货币
        return contract;
    }

    private void sub5sBars() {
        try {
            for (AppConfig.SymbolConfig sc : config.getSymbols()) {
                Contract contract = genContract(sc);
                ic.reqContractDetails(contract, new ContractDetailsHandler());
                boolean rthOnly = false;
                int reqId = ic.reqRealTimeBars(contract, Types.WhatToShow.TRADES, rthOnly, realTimeBarHandler);
                realTimeBarHandler.bindReqIdKey(Utils.genKey(sc.getSymbol(), sc.getSecType()), reqId);
            }
        } catch (Exception e) {
            logger.error("failed to subscribe 5s bar info, err:" + e.getMessage());
        }
    }

    private void subMktData() {
        try {
            for (AppConfig.SymbolConfig sc : config.getSymbols()) {
                Contract contract = genContract(sc);
                // contract, genericTickList, snapshot, regulatorySnapshot, ITopMktDataHandler
                int reqId = ic.reqTopMktData(contract, "", false, false, topMktDataHandler);
                topMktDataHandler.bindReqIdKey(Utils.genKey(sc.getSymbol(), sc.getSecType()), reqId);
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
