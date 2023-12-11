package capital.daphne.algorithms.close;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.algorithms.AlgorithmProcessor;
import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import capital.daphne.models.WapCache;
import capital.daphne.utils.Utils;
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

public class TrailingStop implements AlgorithmProcessor {

    private static final Logger logger = LoggerFactory.getLogger(TrailingStop.class);
    private final AppConfigManager.AppConfig.AlgorithmConfig ac;

    public TrailingStop(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
    }

    @Override
    public Signal getSignal(Table df, int position, int maxPosition, double bidPrice, double askPrice) {
        if (position == 0) {
            return null;
        }

        AppConfigManager.AppConfig.CloseAlgorithmConfig cac = ac.getCloseAlgo();
        Row row = df.row(df.rowCount() - 1);

        double volatility = row.getDouble("volatility");
        double volatilityMultiplier = Utils.calToVolatilityMultiplier(ac.getVolatilityA(), ac.getVolatilityB(), ac.getVolatilityC(), volatility);

        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();

        // 通过redis获取orderList，如果不存在，直接返回无信号
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            List<OrderInfo> orderList = Utils.getOrderList(jedis, accountId, symbol, secType);
            if (orderList == null) {
                return null;
            }
            String maxMinKey = String.format("%s:%s:MAX_MIN_WAP", symbol, secType);
            String storedWapMaxMinJson = jedis.get(maxMinKey);
            logger.info("redis|" + maxMinKey + "|" + storedWapMaxMinJson);
            if (storedWapMaxMinJson == null) {
                return null;
            }

            ObjectMapper objectMapper = new ObjectMapper();
            // logger.info("storedWapMaxMinJson:" + storedWapMaxMinJson);
            WapCache wapMaxMin = objectMapper.readValue(storedWapMaxMinJson, new TypeReference<>() {
            });
            if (wapMaxMin == null || wapMaxMin.getMinWap() == Double.MIN_VALUE || wapMaxMin.getMinWap() == Double.MAX_VALUE) {
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

//            String nowDateTime = row.getString("date_us");
//            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
//            LocalDateTime lastOrderDateTime = LocalDateTime.parse(lodt, formatter);
//            LocalDateTime now = LocalDateTime.parse(nowDateTime, formatter);

            double threshold = volatilityMultiplier * cac.getTrailingStopThreshold();
            double maxWap = wapMaxMin.getMaxWap();
            double minWap = wapMaxMin.getMinWap();
            double vwap = row.getDouble("vwap");
            if (lastOrderDateTime.plusSeconds(cac.getMinDurationBeforeClose()).isBefore(now) &&
                    lastOrderDateTime.plusSeconds(cac.getMaxDurationToClose()).isAfter(now)) {
                logger.warn(String.format("TRAILING_STOP_SIGNAL_CHECK|accountId=%s|symbol=%s|secType=%s|orderId=%s|quantity=%d|" +
                                "position=%d|vwap=%f|bid=%f|ask=%f|volatility=%f|volatilityMultiplier=%f|origThreshold=%f|threshold=%f|" +
                                "maxWap=%f|minWap=%f|bm=%b|sbm=%b",
                        accountId, symbol, secType, lastOrder.getOrderId(), lastOrder.getQuantity(),
                        position, vwap, bidPrice, askPrice, volatility, volatilityMultiplier, cac.getTrailingStopThreshold(), threshold,
                        maxWap, minWap, vwap <= (1 - threshold) * maxWap, vwap >= (1 + threshold) * minWap));

                if (
                        (lastOrder.getQuantity() > 0 && position > 0 && maxWap > 0 && askPrice <= (1 - threshold) * maxWap) ||
                                (lastOrder.getQuantity() < 0 && position < 0 && minWap > 0 && bidPrice >= (1 + threshold) * minWap)
                ) {
                    signal = Utils.fulfillSignal(accountId, symbol, secType, row.getDouble("vwap"), -lastOrder.getQuantity(), Signal.OrderType.CLOSE, "trailingStop");
                    logger.warn(String.format("TRAILING_STOP_SIGNAL_CHECK|generated signal %s %s %s", accountId, symbol, secType));
                }
            }
            return signal;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("get trailingStop close signal failed, accountId=%s, symbol=%s, secType=%s error=%s",
                    accountId, symbol, secType, e.getMessage()));
            return null;
        }
    }
}
