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
    public Signal getSignal(Table df, int position, int maxPosition) {
        if (position == 0) {
            return null;
        }

        AppConfigManager.AppConfig.CloseAlgorithmConfig cac = ac.getCloseAlgo();
        Row row = df.row(df.rowCount() - 1);

        double volatility = row.getDouble("volatility");
        double volatilityMultiplier = Utils.calToVolatilityMultiplier(ac, volatility);

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
            logger.debug("redis|" + maxMinKey + "|" + storedWapMaxMinJson);
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
            if (lastOrderDateTime.plusSeconds(cac.getMinDurationBeforeClose()).isBefore(now) &&
                    lastOrderDateTime.plusSeconds(cac.getMaxDurationToClose()).isAfter(now)) {
//                System.out.println(row.getString("date_us") + "|" + wapMaxMin.getMaxWap() + "|" + row.getDouble("vwap") +
//                        "|" + volatilityMultiplier + "|" + threshold + "|" + (row.getDouble("vwap") <= (1 - threshold) * wapMaxMin.getMaxWap()) +
//                        "|" + (position < 0 && row.getDouble("vwap") >= (1 + threshold) * wapMaxMin.getMinWap()));
                logger.warn(String.format("TRAILING_STOP_SIGNAL_CHECK|accountId=%s|symbol=%s|secType=%s|orderId=%s|quantity=%d|position=%d|maxWap=%f|minWap=%f|bm=%b|sbm=%b",
                        accountId, symbol, secType, lastOrder.getOrderId(), lastOrder.getQuantity(), position,
                        wapMaxMin.getMaxWap(), wapMaxMin.getMinWap(),
                        row.getDouble("vwap") <= (1 - cac.getTrailingStopThreshold()) * wapMaxMin.getMaxWap(),
                        row.getDouble("vwap") >= (1 + cac.getTrailingStopThreshold()) * wapMaxMin.getMinWap()));
                if ((lastOrder.getQuantity() > 0 && position > 0
                        && wapMaxMin.getMaxWap() > 0 && row.getDouble("vwap") <= (1 - threshold) * wapMaxMin.getMaxWap()) ||
                        (lastOrder.getQuantity() < 0 && position < 0)
                                && wapMaxMin.getMinWap() > 0 && row.getDouble("vwap") >= (1 + threshold) * wapMaxMin.getMinWap()) {
                    signal = Utils.fulfillSignal(accountId, symbol, secType, row.getDouble("vwap"), -lastOrder.getQuantity(), Signal.OrderType.CLOSE, "trailingStop");
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
