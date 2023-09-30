package capital.daphne.datasource.ibkr;

import com.ib.client.Decimal;
import com.ib.client.TickAttrib;
import com.ib.client.TickType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class TopMktDataHandler implements IbkrController.ITopMktDataHandler {

    private static final Logger logger = LoggerFactory.getLogger(TopMktDataHandler.class);

    private Map<Integer, Map<String, Double>> tickerReqIdSblMap;

    public TopMktDataHandler() {
        tickerReqIdSblMap = new HashMap<>();
    }

    @Override
    public void tickPrice(int reqId, TickType tickType, double price, TickAttrib tickAttrib) {
        if (tickType.equals(TickType.BID) || tickType.equals(TickType.ASK)) {
            Map<String, Double> priceMap;
            if (tickerReqIdSblMap.containsKey(reqId)) {
                // 获取 reqId 对应的 Map<String, Double>
                priceMap = tickerReqIdSblMap.get(reqId);

                // 根据 tickType 更新对应的价格
                if (tickType.equals(TickType.BID)) {
                    priceMap.put("bidPrice", price);
                } else if (tickType.equals(TickType.ASK)) {
                    priceMap.put("askPrice", price);
                }
            } else {
                // 如果 reqId 不存在于 tickerReqIdSblMap 中，进行初始化
                priceMap = new HashMap<>();
                if (tickType.equals(TickType.BID)) {
                    priceMap.put("bidPrice", price);
                    priceMap.put("askPrice", 0.0); // 初始化 askPrice
                } else if (tickType.equals(TickType.ASK)) {
                    priceMap.put("bidPrice", 0.0); // 初始化 bidPrice
                    priceMap.put("askPrice", price);
                }

                // 将初始化后的 priceMap 放入 tickerReqIdSblMap
                tickerReqIdSblMap.put(reqId, priceMap);
            }
            logger.debug(String.format("reqId=%d, bidPrice=%f, askPrice=%f", reqId, priceMap.get("bidPrice"), priceMap.get("askPrice")));
        }
        // todo maybe add bidSize and askSize later
    }

    public double getBidPrice(int reqId) {
        if (tickerReqIdSblMap.containsKey(reqId)) {
            Map<String, Double> priceMap = tickerReqIdSblMap.get(reqId);
            return priceMap.getOrDefault("bidPrice", 0.0);
        } else {
            return 0.0;
        }
    }

    public double getAskPrice(int reqId) {
        if (tickerReqIdSblMap.containsKey(reqId)) {
            Map<String, Double> priceMap = tickerReqIdSblMap.get(reqId);
            return priceMap.getOrDefault("askPrice", 0.0);
        } else {
            return 0.0;
        }
    }

    @Override
    public void tickSize(TickType tickType, Decimal size) {
    }

    @Override
    public void tickString(TickType tickType, String value) {
    }

    @Override
    public void tickSnapshotEnd() {
        System.out.println("tickSnapshotEnd");
    }

    @Override
    public void marketDataType(int marketDataType) {
    }

    @Override
    public void tickReqParams(int tickerId, double minTick, String bboExchange, int snapshotPermissions) {
    }
}
