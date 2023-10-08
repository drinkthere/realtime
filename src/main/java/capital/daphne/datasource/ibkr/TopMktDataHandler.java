package capital.daphne.datasource.ibkr;

import capital.daphne.AppConfig;
import capital.daphne.JedisUtil;
import capital.daphne.utils.Utils;
import com.ib.client.Decimal;
import com.ib.client.TickAttrib;
import com.ib.client.TickType;
import com.mysql.cj.util.StringUtils;
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

    private Map<Integer, String> reqIdSblMap;
    private Map<String, Integer> sblReqIdMap;

    private Map<Integer, Map<String, Double>> tickerReqIdSblMap;

    // 用来计算twap
    private Map<String, Double> twapMap;

    private List<AppConfig.SymbolConfig> symbolsConfig;

    public TopMktDataHandler(List<AppConfig.SymbolConfig> symbolsConfig) {
        this.symbolsConfig = symbolsConfig;
        tickerReqIdSblMap = new HashMap<>();
        reqIdSblMap = new HashMap<>();
        sblReqIdMap = new HashMap<>();
        twapMap = new HashMap<>();
    }

    @Override
    public void tickPrice(int reqId, TickType tickType, double price, TickAttrib tickAttrib) {
        logger.debug(reqId + " " + tickType + " " + price + " " + tickAttrib);
        // key = symbol + "." + secType, 如: SPY.STK 或 SPY.CDF 等
        String key = reqIdSblMap.get(reqId);
        List<String> splitArr = StringUtils.split(key, ".", true);
        String symbol = splitArr.get(0);
        String secType = splitArr.get(1);

        if (tickType.equals(TickType.BID) || tickType.equals(TickType.ASK)) {
            if (price <= 0.0) {
                logger.warn(reqId + " " + tickType + " " + price + " " + tickAttrib);
                return;
            }
            try {
                Map<String, Double> priceMap;
                if (tickerReqIdSblMap.containsKey(reqId)) {
                    // 获取 reqId 对应的 Map<String, Double>
                    priceMap = tickerReqIdSblMap.get(reqId);

                    // 根据 tickType 更新对应的价格
                    if (tickType.equals(TickType.BID)) {
                        priceMap.put("bidPrice", price);
                        updateTickerInRedis(symbol, secType, TickType.BID, price);
                    } else if (tickType.equals(TickType.ASK)) {
                        priceMap.put("askPrice", price);
                        updateTickerInRedis(symbol, secType, TickType.ASK, price);
                    }
                } else {
                    // 如果 reqId 不存在于 tickerReqIdSblMap 中，进行初始化
                    priceMap = new HashMap<>();
                    if (tickType.equals(TickType.BID)) {
                        priceMap.put("bidPrice", price);
                        priceMap.put("askPrice", 0.0); // 初始化 askPrice
                        updateTickerInRedis(symbol, secType, TickType.BID, price);
                        updateTickerInRedis(symbol, secType, TickType.ASK, 0.0);
                    } else if (tickType.equals(TickType.ASK)) {
                        priceMap.put("bidPrice", 0.0); // 初始化 bidPrice
                        priceMap.put("askPrice", price);
                        updateTickerInRedis(symbol, secType, TickType.BID, 0.0);
                        updateTickerInRedis(symbol, secType, TickType.ASK, price);
                    }

                    // 将初始化后的 priceMap 放入 tickerReqIdSblMap
                    tickerReqIdSblMap.put(reqId, priceMap);
                }
                // updateTwap(reqId, priceMap);
                logger.debug(String.format("reqId=%d, symbol=%s, secType=%s, bidPrice=%f, askPrice=%f",
                        reqId, symbol, secType, priceMap.get("bidPrice"), priceMap.get("askPrice")));
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
        // todo maybe add bidSize and askSize later
    }


    public double getTwap(String symbol, String secType) {
        String key = Utils.genKey(symbol, secType);
        Double twap = twapMap.get(key);
        return (twap == null) ? 0.0 : twap;
    }

    public void bindReqIdSymbol(String symbol, String secType, int reqId) {
        String key = Utils.genKey(symbol, secType);
        sblReqIdMap.put(key, reqId);
        reqIdSblMap.put(reqId, key);
    }

    public double getBidPrice(String symbol, String secType) {
        String key = Utils.genKey(symbol, secType);
        Integer reqId = sblReqIdMap.get(key);
        if (tickerReqIdSblMap.containsKey(reqId)) {
            Map<String, Double> priceMap = tickerReqIdSblMap.get(reqId);
            return priceMap.getOrDefault("bidPrice", 0.0);
        } else {
            return 0.0;
        }
    }

    public double getAskPrice(String symbol, String secType) {
        String key = Utils.genKey(symbol, secType);
        Integer reqId = sblReqIdMap.get(key);
        if (tickerReqIdSblMap.containsKey(reqId)) {
            Map<String, Double> priceMap = tickerReqIdSblMap.get(reqId);
            return priceMap.getOrDefault("askPrice", 0.0);
        } else {
            return 0.0;
        }
    }

    private void updateTickerInRedis(String symbol, String secType, TickType type, double price) {
        JedisPool jedisPool = JedisUtil.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {

            // 更新ticker信息，设置10s过期
            String key = symbol + "." + secType + "." + type;
            String val = String.valueOf(price);
            jedis.set(key, val);
            long timestamp = System.currentTimeMillis() / 1000 + 10;
            jedis.expireAt("mykey", timestamp);

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
                key = rewrite.getSymbol() + "." + rewrite.getSecType() + "." + type;
                jedis.set(key, val);
                jedis.expireAt(key, timestamp);
            }

            AppConfig.Parallel parallel = sc.getParallel();
            if (parallel != null) {
                key = parallel.getSymbol() + "." + parallel.getSecType() + "." + type;
                jedis.set(key, val);
                jedis.expireAt(key, timestamp);
            }
        } catch (Exception e) {
            logger.error("Get Jedis resource failed, error:" + e.getMessage());
        }
    }

    private void updateTwap(int reqId, Map<String, Double> priceMap) {
        String key = reqIdSblMap.get(reqId);
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
