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

    private Strategy strategyHandler;

    private Map<String, Integer> tickerSblReqIdMap;


    private Map<String, Integer> barSblReqIdMap;


    public Ibkr(AppConfig appConfig, Db dbHandler) {
        ConnectHandler connectHandler = new ConnectHandler();
        ic = new IbkrController(connectHandler);
        config = appConfig;
        db = dbHandler;
        topMktDataHandler = new TopMktDataHandler();
        positionHandler = new PositionHandler();
        tickerSblReqIdMap = new HashMap<>();
        strategyHandler = initStrategyHandler(config.getStrategy());

        List<String> symbols = config.getSymbols().stream()
                .map(AppConfig.SymbolItem::getSymbol)
                .collect(Collectors.toList());
        realTimeBarHandler = new RealTimeBarHandler(db, symbols, config.getNumStatsBars() + 1);
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
        logger.info("订阅5s bar数据");
        sub5sBars();

        logger.info("订阅ticker数据");
        subMktData();

        logger.info("订阅position数据");
        subPosition();
    }

    public List<Signal> getTradeSignals() {
        List<Signal> signalList = new ArrayList<>();

        for (AppConfig.SymbolItem symbolItem : config.getSymbols()) {
            Signal signal = new Signal();
            signal.setValid(false);
            String symbol = symbolItem.getSymbol();

            // 获取position信息
            int position = positionHandler.getSymbolPosition(symbol);

            // 获取5s bar信息
            Table df = realTimeBarHandler.getDataTable(symbol);
            if (df == null) {
                return signalList;
            }
            // logger.info(symbol + "," + df.toString());

            // 获取当前bid和ask信息
            double bidPrice = topMktDataHandler.getBidPrice(tickerSblReqIdMap.get(symbol));
            double askPrice = topMktDataHandler.getAskPrice(tickerSblReqIdMap.get(symbol));

            // 判断是否要下单
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
            signal.setQuantity(symbolItem.getOrderSize());
            signalList.add(signal);
        }
        return signalList;
    }

    private Contract genContract(String symbol) {
        AppConfig.SymbolItem symbolInfo = new AppConfig.SymbolItem();
        for (AppConfig.SymbolItem symbolItem : config.getSymbols()) {
            if (symbolItem.getSymbol().equals(symbol)) {
                symbolInfo = symbolItem;
                break;
            }
        }

        Contract contract = new Contract();
        contract.symbol(symbol); // 设置合约标的
        contract.secType(Types.SecType.STK);
        contract.exchange(symbolInfo.getExchange()); // 设置交易所
        contract.primaryExch(symbolInfo.getPrimaryExchange());
        contract.currency(symbolInfo.getCurrency()); // 设置货币
        return contract;
    }

    private Strategy initStrategyHandler(String strategyStr) {
        return new Sma(config);
    }

    private void sub5sBars() {
        try {
            for (AppConfig.SymbolItem symbolItem : config.getSymbols()) {
                String symbol = symbolItem.getSymbol();
                Contract contract = genContract(symbol);
                boolean rthOnly = true;
                int reqId = ic.reqRealTimeBars(contract, Types.WhatToShow.TRADES, rthOnly, realTimeBarHandler);
                barSblReqIdMap.put(symbol, reqId);
                realTimeBarHandler.setBarSblReqIdMap(barSblReqIdMap);
            }
        } catch (Exception e) {
            logger.error("failed to subscribe 5s bar info, err:" + e.getMessage());
        }
    }

    private void subMktData() {
        try {
            for (AppConfig.SymbolItem symbolItem : config.getSymbols()) {
                String symbol = symbolItem.getSymbol();
                Contract contract = genContract(symbol);
                // contract, genericTickList, snapshot, regulatorySnapshot, ITopMktDataHandler
                int reqId = ic.reqTopMktData(contract, "", false, false, topMktDataHandler);
                tickerSblReqIdMap.put(symbol, reqId);
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
