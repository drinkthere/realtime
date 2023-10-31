package capital.daphne.algorithms.close;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.algorithms.Sma;
import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import capital.daphne.models.WapMaxMin;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

public class TrailingStop implements CloseAlgorithm {

    private static final Logger logger = LoggerFactory.getLogger(Sma.class);
    private final AppConfigManager.AppConfig.AlgorithmConfig ac;

    public TrailingStop(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
    }

    @Override
    public Signal getSignal(Table df, int position, int maxPosition) {
        if (position == 0) {
            return null;
        }

        AppConfigManager.AppConfig.CloseAlgorithmConfig cac = ac.getCloseAlgo();
        Row row = df.row(df.rowCount() - 1);

        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();

        // 通过redis获取orderList，如果不存在，直接返回无信号
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = accountId + "." + symbol + "." + secType + ".ORDER_LIST";
            String storedOrderListJson = jedis.get(redisKey);
            logger.info("redis|" + redisKey + "|" + storedOrderListJson);
            if (storedOrderListJson == null) {
                return null;
            }

            ObjectMapper objectMapper = new ObjectMapper();
            List<OrderInfo> orderList = objectMapper.readValue(storedOrderListJson, new TypeReference<>() {
            });
            if (orderList == null && orderList.size() == 0) {
                return null;
            }

            String maxMinKey = String.format("%s.%s:MAX_MIN_WAP", symbol, secType);
            String storedWapMaxMinJson = jedis.get(maxMinKey);
            logger.info("redis|" + maxMinKey + "|" + storedWapMaxMinJson);
            if (storedWapMaxMinJson == null) {
                return null;
            }

            objectMapper = new ObjectMapper();
            WapMaxMin wapMaxMin = objectMapper.readValue(storedWapMaxMinJson, new TypeReference<>() {
            });
            if (wapMaxMin == null || wapMaxMin.getMaxPriceSinceLastOrder() == Double.MIN_VALUE || wapMaxMin.getMinPriceSinceLastOrder() == Double.MAX_VALUE) {
                return null;
            }

            // 如果有数据，判断是否针对最后一笔交易进行减仓（后需可扩展）
            Signal signal = new Signal();
            signal.setValid(false);
            OrderInfo lastOrder = orderList.get(orderList.size() - 1);
            String lodt = lastOrder.getDateTime();

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
            LocalDateTime lastOrderDateTime = LocalDateTime.parse(lodt, formatter);
            LocalDateTime now = LocalDateTime.now();

            if (lastOrderDateTime.plusSeconds(cac.getMinDurationBeforeClose()).isBefore(now) &&
                    lastOrderDateTime.plusSeconds(cac.getMaxDurationToClose()).isAfter(now)) {
                logger.info(String.format("TRAILING_STOP_SIGNAL_CHECK|accountId=%s|symbol=%s|secType=%s|orderId=%s|quantity=%d|position=%d|maxWap=%f|minWap=%f|bm=%f|sbm=%f|%s",
                        accountId, symbol, secType, lastOrder.getOrderId(), lastOrder.getQuantity(), position,
                        wapMaxMin.getMaxPriceSinceLastOrder(), wapMaxMin.getMinPriceSinceLastOrder(),
                        row.getDouble("vwap") <= (1 - cac.getTrailingStopThreshold()) * wapMaxMin.getMaxPriceSinceLastOrder(),
                        row.getDouble("vwap") >= (1 + cac.getTrailingStopThreshold()) * wapMaxMin.getMinPriceSinceLastOrder()));
                if ((lastOrder.getQuantity() > 0 && position > 0
                        && wapMaxMin.getMaxPriceSinceLastOrder() > 0 && row.getDouble("vwap") <= (1 - cac.getTrailingStopThreshold()) * wapMaxMin.getMaxPriceSinceLastOrder()) ||
                        (lastOrder.getQuantity() < 0 && position < 0)
                                && wapMaxMin.getMinPriceSinceLastOrder() > 0 && row.getDouble("vwap") >= (1 + cac.getTrailingStopThreshold()) * wapMaxMin.getMinPriceSinceLastOrder()) {
                    signal.setValid(true);
                    signal.setAccountId(accountId);
                    signal.setUuid(UUID.randomUUID().toString());
                    signal.setSymbol(symbol);
                    signal.setSecType(secType);
                    signal.setWap(row.getDouble("vwap"));
                    signal.setQuantity(-lastOrder.getQuantity());
                    signal.setOrderType(Signal.OrderType.CLOSE);
                    signal.setBenchmarkColumn("trailingStop");
                }
            }
            return signal;
        } catch (Exception e) {
            logger.error(String.format("get order list in redis failed, accountId=%s, symbol=%s, secType=%s error=%s",
                    accountId, symbol, secType, e.getMessage()));
            return null;
        }
    }
}
