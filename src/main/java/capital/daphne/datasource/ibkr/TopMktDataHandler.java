package capital.daphne.datasource.ibkr;

import capital.daphne.AppConfig;
import capital.daphne.JedisUtil;
import com.ib.client.Decimal;
import com.ib.client.TickAttrib;
import com.ib.client.TickType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TopMktDataHandler implements IbkrController.ITopMktDataHandler {

    private static final Logger logger = LoggerFactory.getLogger(TopMktDataHandler.class);

    private final Map<Integer, String> reqIdKeyMap;
    private final Map<String, Integer> keyReqIdMap;

    private final Map<Integer, Map<String, Double>> reqIdTickerMap;

    // 用来计算twap
    private final Map<String, Double> twapMap;

    private final List<AppConfig.SymbolConfig> symbolsConfig;

    public TopMktDataHandler(List<AppConfig.SymbolConfig> symbolsConfig) {
        this.symbolsConfig = symbolsConfig;
        reqIdTickerMap = new HashMap<>();
        reqIdKeyMap = new HashMap<>();
        keyReqIdMap = new HashMap<>();
        twapMap = new HashMap<>();
    }

    @Override
    public void tickPrice(int reqId, TickType tickType, double price, TickAttrib tickAttrib) {
        logger.debug(reqId + " " + tickType + " " + price + " " + tickAttrib);
        // key = symbol + "." + secType, 如: SPY.STK 或 SPY.CDF 等
        String key = reqIdKeyMap.get(reqId);
        if (tickType.equals(TickType.BID) || tickType.equals(TickType.ASK)) {
            if (price <= 0.0) {
                logger.warn(reqId + " " + tickType + " " + price + " " + tickAttrib);
                return;
            }
            try {
                Map<String, Double> priceMap;
                if (reqIdTickerMap.containsKey(reqId)) {
                    // 获取 reqId 对应的 Map<String, Double>
                    priceMap = reqIdTickerMap.get(reqId);

                    // 根据 tickType 更新对应的价格
                    if (tickType.equals(TickType.BID)) {
                        priceMap.put("bidPrice", price);
                        updateTickerInRedis(key, TickType.BID, price);
                    } else {
                        priceMap.put("askPrice", price);
                        updateTickerInRedis(key, TickType.ASK, price);
                    }
                } else {
                    // 如果 reqId 不存在于 reqIdTickerMap 中，进行初始化
                    priceMap = new HashMap<>();
                    if (tickType.equals(TickType.BID)) {
                        priceMap.put("bidPrice", price);
                        priceMap.put("askPrice", 0.0); // 初始化 askPrice
                        updateTickerInRedis(key, TickType.BID, price);
                        updateTickerInRedis(key, TickType.ASK, 0.0);
                    } else {
                        priceMap.put("bidPrice", 0.0); // 初始化 bidPrice
                        priceMap.put("askPrice", price);
                        updateTickerInRedis(key, TickType.BID, 0.0);
                        updateTickerInRedis(key, TickType.ASK, price);
                    }

                    // 将初始化后的 priceMap 放入 reqIdTickerMap
                    reqIdTickerMap.put(reqId, priceMap);
                }
                // updateTwap(reqId, priceMap);
                logger.debug(String.format("reqId=%d, key=%s, bidPrice=%f, askPrice=%f",
                        reqId, key, priceMap.get("bidPrice"), priceMap.get("askPrice")));
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
        // todo maybe add bidSize and askSize later
    }


    public double getTwap(String key) {
        Double twap = twapMap.get(key);
        return (twap == null) ? 0.0 : twap;
    }

    public void bindReqIdKey(String key, int reqId) {
        keyReqIdMap.put(key, reqId);
        reqIdKeyMap.put(reqId, key);
    }

    public double getBidPrice(String key) {
        Integer reqId = keyReqIdMap.get(key);
        if (reqIdTickerMap.containsKey(reqId)) {
            Map<String, Double> priceMap = reqIdTickerMap.get(reqId);
            return priceMap.getOrDefault("bidPrice", 0.0);
        } else {
            return 0.0;
        }
    }

    public double getAskPrice(String key) {
        Integer reqId = keyReqIdMap.get(key);
        if (reqIdTickerMap.containsKey(reqId)) {
            Map<String, Double> priceMap = reqIdTickerMap.get(reqId);
            return priceMap.getOrDefault("askPrice", 0.0);
        } else {
            return 0.0;
        }
    }

    private void updateTickerInRedis(String key, TickType type, double price) {
        JedisPool jedisPool = JedisUtil.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            // 更新ticker信息，设置10s过期
            String redisKey = key + "." + type;
            String val = String.valueOf(price);
            jedis.set(redisKey, val);
            long timestamp = System.currentTimeMillis() / 1000 + 10;
            jedis.expireAt("mykey", timestamp);

            String[] splitArr = key.split("\\.");
            String symbol = splitArr[0];
            String secType = splitArr[1];
            AppConfig.SymbolConfig sc;
            Optional<AppConfig.SymbolConfig> symbolItemOptional = symbolsConfig.stream()
                    .filter(item -> item.getSymbol().equals(symbol) && item.getSecType().equals(secType))
                    .findFirst();
            if (symbolItemOptional.isPresent()) {
                sc = symbolItemOptional.get();
            } else {
                logger.error("Can't find the configuration of symbol=" + symbol + ", secType=" + secType);
                return;
            }

            // 如果有需要重写的，或者并行发送的contract，就把对应的价格也存储下来
            AppConfig.Rewrite rewrite = sc.getRewrite();
            if (rewrite != null) {
                redisKey = rewrite.getSymbol() + "." + rewrite.getSecType() + "." + type;
                jedis.set(redisKey, val);
                jedis.expireAt(redisKey, timestamp);
            }

            AppConfig.Parallel parallel = sc.getParallel();
            if (parallel != null) {
                redisKey = parallel.getSymbol() + "." + parallel.getSecType() + "." + type;
                jedis.set(redisKey, val);
                jedis.expireAt(redisKey, timestamp);
            }
        } catch (Exception e) {
            logger.error("Get Jedis resource failed, error:" + e.getMessage());
        }
    }

    private void updateTwap(int reqId, Map<String, Double> priceMap) {
        String key = reqIdKeyMap.get(reqId);
        Double bidPrice = priceMap.get("bidPrice");
        Double askPrice = priceMap.get("askPrice");
        if (bidPrice <= 0.0 || askPrice <= 0.0) {
            return;
        }
        Double twap = (bidPrice + askPrice) / 2;
        twapMap.put(key, twap);
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
