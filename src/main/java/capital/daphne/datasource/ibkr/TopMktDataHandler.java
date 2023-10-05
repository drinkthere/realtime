package capital.daphne.datasource.ibkr;

import capital.daphne.JedisUtil;
import com.ib.client.Decimal;
import com.ib.client.TickAttrib;
import com.ib.client.TickType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

public class TopMktDataHandler implements IbkrController.ITopMktDataHandler {

    private static final Logger logger = LoggerFactory.getLogger(TopMktDataHandler.class);

    private Map<Integer, String> reqIdSblMap;
    private Map<String, Integer> sblReqIdMap;

    private Map<Integer, Map<String, Double>> tickerReqIdSblMap;

    public TopMktDataHandler() {
        tickerReqIdSblMap = new HashMap<>();
        reqIdSblMap = new HashMap<>();
        sblReqIdMap = new HashMap<>();
    }

    @Override
    public void tickPrice(int reqId, TickType tickType, double price, TickAttrib tickAttrib) {
        logger.debug(reqId + " " + tickType + " " + price + " " + tickAttrib);
        String symbol = reqIdSblMap.get(reqId);
        if (tickType.equals(TickType.BID) || tickType.equals(TickType.ASK)) {
            try {
                Map<String, Double> priceMap;
                if (tickerReqIdSblMap.containsKey(reqId)) {
                    // 获取 reqId 对应的 Map<String, Double>
                    priceMap = tickerReqIdSblMap.get(reqId);

                    // 根据 tickType 更新对应的价格
                    if (tickType.equals(TickType.BID)) {
                        priceMap.put("bidPrice", price);
                        updateTickerInRedis(symbol, TickType.BID, price);
                    } else if (tickType.equals(TickType.ASK)) {
                        priceMap.put("askPrice", price);
                        updateTickerInRedis(symbol, TickType.ASK, price);
                    }
                } else {
                    // 如果 reqId 不存在于 tickerReqIdSblMap 中，进行初始化
                    priceMap = new HashMap<>();
                    if (tickType.equals(TickType.BID)) {
                        priceMap.put("bidPrice", price);
                        priceMap.put("askPrice", 0.0); // 初始化 askPrice
                        updateTickerInRedis(symbol, TickType.BID, price);
                        updateTickerInRedis(symbol, TickType.ASK, 0.0);
                    } else if (tickType.equals(TickType.ASK)) {
                        priceMap.put("bidPrice", 0.0); // 初始化 bidPrice
                        priceMap.put("askPrice", price);
                        updateTickerInRedis(symbol, TickType.BID, 0.0);
                        updateTickerInRedis(symbol, TickType.ASK, price);
                    }

                    // 将初始化后的 priceMap 放入 tickerReqIdSblMap
                    tickerReqIdSblMap.put(reqId, priceMap);
                }
                logger.debug(String.format("reqId=%d, bidPrice=%f, askPrice=%f", reqId, priceMap.get("bidPrice"), priceMap.get("askPrice")));
            } catch (Exception e) {
                logger.error(e.getMessage());
            }

        }
        // todo maybe add bidSize and askSize later
    }

    private void updateTickerInRedis(String symbol, TickType type, double price) {
        JedisPool jedisPool = JedisUtil.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            // 更新ticker信息，设置10s过期
            String key = symbol + "." + type;
            jedis.set(key, String.valueOf(price));
            long timestamp = System.currentTimeMillis() + 10000;
            jedis.expireAt(key, timestamp);
        } catch (Exception e) {
            logger.error("Get Jedis resource failed, error:" + e.getMessage());
        }
    }

    public void bindReqIdSymbol(String symbol, int reqId) {
        sblReqIdMap.put(symbol, reqId);
        reqIdSblMap.put(reqId, symbol);
    }

    public double getBidPrice(String symbol) {
        Integer reqId = sblReqIdMap.get(symbol);
        if (tickerReqIdSblMap.containsKey(reqId)) {
            Map<String, Double> priceMap = tickerReqIdSblMap.get(reqId);
            return priceMap.getOrDefault("bidPrice", 0.0);
        } else {
            return 0.0;
        }
    }

    public double getAskPrice(String symbol) {
        Integer reqId = sblReqIdMap.get(symbol);
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
