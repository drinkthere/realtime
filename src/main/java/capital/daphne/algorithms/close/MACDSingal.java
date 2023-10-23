package capital.daphne.algorithms.close;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.algorithms.Sma;
import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import capital.daphne.utils.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

public class MACDSingal implements CloseAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(Sma.class);
    private final AppConfigManager.AppConfig.AlgorithmConfig ac;
    private final AppConfigManager.AppConfig.CloseAlgorithmConfig cac;

    private final String shortBenchmarkColumn;
    private final String longBenchmarkColumn;
    private final String signalBenchmarkColumn;
    private final String benchmarkColumn;

    public MACDSingal(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
        cac = ac.getCloseAlgo();

        shortBenchmarkColumn = "MACDShort" + cac.getMacdShortNumStatsBar();
        longBenchmarkColumn = "MACDLong" + cac.getMacdLongNumStatsBar();
        signalBenchmarkColumn = "MACDSignal" + cac.getMacdSignalNumStatsBar();
        benchmarkColumn = "MACDLine";
    }

    public Signal getSignal(Table inputDf, int position, int maxPosition) {
        Table df = generateBenchmarkColumn(inputDf);
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

            if (lastOrderDateTime.plusSeconds(cac.getMinDurationBeforeClose()).isBefore(now) &&
                    lastOrderDateTime.plusSeconds(cac.getMaxDurationToClose()).isAfter(now)) {
                logger.info(String.format("MACD_SIGNAL_CHECK|accountId=%s|symbol=%s|secType=%s|orderId=%s|quantity=%d|position=%d|bm=%f|sbm=%f|%s",
                        accountId, symbol, secType, lastOrder.getOrderId(), lastOrder.getQuantity(), position, row.getDouble(benchmarkColumn), row.getDouble(signalBenchmarkColumn),
                        (lastOrder.getQuantity() > 0 && row.getDouble(benchmarkColumn) < row.getDouble(signalBenchmarkColumn) && position > 0) ||
                                (lastOrder.getQuantity() < 0 && row.getDouble(benchmarkColumn) > row.getDouble(signalBenchmarkColumn) && position < 0)));
                if ((lastOrder.getQuantity() > 0 && row.getDouble(benchmarkColumn) < row.getDouble(signalBenchmarkColumn) && position > 0) ||
                        (lastOrder.getQuantity() < 0 && row.getDouble(benchmarkColumn) > row.getDouble(signalBenchmarkColumn) && position < 0)) {
                    signal.setValid(true);
                    signal.setAccountId(accountId);
                    signal.setUuid(UUID.randomUUID().toString());
                    signal.setSymbol(symbol);
                    signal.setSecType(secType);
                    signal.setWap(row.getDouble("vwap"));
                    signal.setQuantity(-lastOrder.getQuantity());
                    signal.setOrderType(Signal.OrderType.CLOSE);

                    jedis.del(redisKey);
                }
            } else {
                logger.info(String.format("MACD_SIGNAL_EXPIRED|accountId=%s|symbol=%s|secType=%s|orderId=%s|order_datetime=%s|now=%s",
                        accountId, symbol, secType, lastOrder.getOrderId(), lastOrderDateTime, now));
            }
            return signal;
        } catch (Exception e) {
            logger.error(String.format("get order list in redis failed, accountId=%s, symbol=%s, secType=%s error=%s",
                    accountId, symbol, secType, e.getMessage()));
            return null;
        }
    }

    public Table generateBenchmarkColumn(Table df) {
        AppConfigManager.AppConfig.CloseAlgorithmConfig closeAlgo = ac.getCloseAlgo();
        int shortPeriod = closeAlgo.getMacdShortNumStatsBar();
        double shortMultiplier = 2.0 / (shortPeriod + 1);

        int longPeriod = closeAlgo.getMacdLongNumStatsBar();
        double longMultiplier = 2.0 / (longPeriod + 1);

        int signalPeriod = closeAlgo.getMacdSignalNumStatsBar();
        double signalMultiplier = 2.0 / (signalPeriod + 1);

        // Seed the EMA with the SMA value for its first data point
        DoubleColumn prev_vwap = df.doubleColumn("prev_vwap");
        DoubleColumn shortEma = Utils.ewm(prev_vwap, shortMultiplier, shortBenchmarkColumn, true, false, shortPeriod, shortPeriod - 1);
        DoubleColumn longEma = Utils.ewm(prev_vwap, longMultiplier, longBenchmarkColumn, true, false, longPeriod, longPeriod - 1);

        DoubleColumn macdLine = shortEma.subtract(longEma);
        macdLine.setName(benchmarkColumn);
        df.addColumns(macdLine);


        DoubleColumn macdSignal = Utils.ewm(macdLine, signalMultiplier, signalBenchmarkColumn, true, false, signalPeriod, signalPeriod - 1);
        df.addColumns(macdSignal);
        return df;
    }
}
