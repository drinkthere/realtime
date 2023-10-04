package capital.daphne.datasource.ibkr;

import capital.daphne.AppConfig;
import capital.daphne.Db;
import capital.daphne.datasource.Signal;
import capital.daphne.strategy.Sma;
import capital.daphne.strategy.Strategy;
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
import java.util.stream.Collectors;


public class Ibkr {
    private static final Logger logger = LoggerFactory.getLogger(Ibkr.class);

    private IbkrController ic;

    private AppConfig config;

    private Db db;
    private TopMktDataHandler topMktDataHandler;

    private RealTimeBarHandler realTimeBarHandler;

    private PositionHandler positionHandler;


    private Map<String, Integer> barSblReqIdMap;


    public Ibkr(AppConfig appConfig, Db dbHandler) {
        ConnectHandler connectHandler = new ConnectHandler();
        ic = new IbkrController(connectHandler);
        config = appConfig;
        db = dbHandler;
        topMktDataHandler = new TopMktDataHandler();
        positionHandler = new PositionHandler();

        List<String> symbols = config.getSymbols().stream()
                .map(AppConfig.SymbolConfig::getSymbol)
                .collect(Collectors.toList());
        realTimeBarHandler = new RealTimeBarHandler(db, config.getSymbols());
        barSblReqIdMap = new HashMap<>();

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

        for (AppConfig.SymbolConfig symbolConfig : config.getSymbols()) {
            Signal signal = new Signal();
            signal.setValid(false);
            String symbol = symbolConfig.getSymbol();

            // 获取position信息
            int position = positionHandler.getSymbolPosition(symbol);

            // 获取5s bar信息
            Table df = realTimeBarHandler.getDataTable(symbol);
            if (df == null) {
                return signalList;
            }
            // logger.info(symbol + "," + df.toString());

            // 获取当前bid和ask信息
            double bidPrice = topMktDataHandler.getBidPrice(symbol);
            double askPrice = topMktDataHandler.getAskPrice(symbol);

            // 判断是否要下单
            Strategy strategyHandler = initStrategyHandler(symbolConfig.getStrategy());
            Strategy.TradeActionType side = strategyHandler.getSignalSide(symbol, df, position);
            if (side == Strategy.TradeActionType.NO_ACTION) {
                return signalList;
            }

            Row latestBar = df.row(df.rowCount() - 1);
            signal.setValid(true);
            signal.setSymbol(symbol);
            signal.setSide(side);
            signal.setBidPrice(bidPrice);
            signal.setAskPrice(askPrice);
            signal.setWap(latestBar.getDouble("vwap"));
            signal.setQuantity(symbolConfig.getOrderSize());
            signalList.add(signal);
        }
        return signalList;
    }

    private Contract genContract(String symbol) {
        AppConfig.SymbolConfig symbolConfig = new AppConfig.SymbolConfig();
        for (AppConfig.SymbolConfig sc : config.getSymbols()) {
            if (sc.getSymbol().equals(symbol)) {
                symbolConfig = sc;
                break;
            }
        }

        Contract contract = new Contract();
        contract.symbol(symbol); // 设置合约标的

        if (symbolConfig.getSecType().equals("Future")) {
            contract.secType(Types.SecType.FUT);
            contract.lastTradeDateOrContractMonth(symbolConfig.getLastTradeDateOrContractMonth());
            contract.multiplier(symbolConfig.getMultiplier());
        } else if (symbolConfig.getSecType().equals("Stock")) {
            contract.secType(Types.SecType.STK);
        }
        contract.exchange(symbolConfig.getExchange()); // 设置交易所
        contract.primaryExch(symbolConfig.getPrimaryExchange());
        contract.currency(symbolConfig.getCurrency()); // 设置货币
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
            for (AppConfig.SymbolConfig symbolConfig : config.getSymbols()) {
                String symbol = symbolConfig.getSymbol();
                Contract contract = genContract(symbol);
                boolean rthOnly = true;
                int reqId = ic.reqRealTimeBars(contract, Types.WhatToShow.TRADES, rthOnly, realTimeBarHandler);
                barSblReqIdMap.put(symbol, reqId);
                realTimeBarHandler.setSblReqIdMap(barSblReqIdMap);
            }
        } catch (Exception e) {
            logger.error("failed to subscribe 5s bar info, err:" + e.getMessage());
        }
    }

    private void subMktData() {
        try {
            for (AppConfig.SymbolConfig symbolConfig : config.getSymbols()) {
                String symbol = symbolConfig.getSymbol();
                Contract contract = genContract(symbol);
                // contract, genericTickList, snapshot, regulatorySnapshot, ITopMktDataHandler
                int reqId = ic.reqTopMktData(contract, "", false, false, topMktDataHandler);
                topMktDataHandler.bindReqIdSymbol(symbol, reqId);
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
