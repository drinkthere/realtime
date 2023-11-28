package capital.daphne.utils;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.models.ActionInfo;
import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import capital.daphne.models.TradingHours;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.columns.numbers.DoubleColumnType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

public class Utils {
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);

    public static LocalDateTime getMarketOpenTime(String symbol, String secType) {
        LocalDate today = LocalDate.now();
        LocalTime time = LocalTime.of(9, 30, 0);
        LocalDateTime marketOpenTime = LocalDateTime.of(today, time);


        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            // 兼容parallel/rewrite
            String mapKey = String.format("%s:%s:KEY_MAP", symbol, secType);
            String key = jedis.get(mapKey);

            String redisKey = String.format("%s:TRADING_PERIODS", key);
            String tradingHoursStr = jedis.get(redisKey);
            if (tradingHoursStr != null) {
                TradingHours[] tradingHours = parseTradingHours(tradingHoursStr, secType);
                for (TradingHours tradingHour : tradingHours) {
                    LocalDateTime now = genUsDateTimeNow();
                    if (now.toLocalDate().equals(tradingHour.getStartTime().toLocalDate())) {
                        return tradingHour.getStartTime();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return marketOpenTime;
    }

    public static boolean isTradingNow(String symbol, String secType, LocalDateTime currentTime, int startTradingAfterOpenMarketSeconds) {
        boolean open = false;
        String redisKey = String.format("%s:%s:TRADING_PERIODS", symbol, secType);
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String tradingHoursStr = jedis.get(redisKey);
            logger.debug("tradingHoursStr:" + tradingHoursStr);
            if (tradingHoursStr != null) {
                TradingHours[] tradingHours = parseTradingHours(tradingHoursStr, secType);
                for (TradingHours tradingHour : tradingHours) {
                    if (tradingHour.isClosed()) {
                        continue;
                    }

                    logger.debug("tradingHour is Closed:" + (currentTime.isAfter(tradingHour.getStartTime()) || currentTime.isEqual(tradingHour.getStartTime())));
                    LocalDateTime startTradingTime = tradingHour.getStartTime().plusSeconds(startTradingAfterOpenMarketSeconds);
                    if ((currentTime.isAfter(startTradingTime) || currentTime.isEqual(startTradingTime)) &&
                            currentTime.isBefore(tradingHour.getEndTime())) {
                        return true;
                    }
                }
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return open;
    }

    private static TradingHours[] parseTradingHours(String tradingHoursStr, String secType) {
        String[] segments = tradingHoursStr.split(";");
        TradingHours[] tradingHours = new TradingHours[segments.length];
        for (int i = 0; i < segments.length; i++) {
            String[] startAndEnd = segments[i].split("-");
            if (startAndEnd.length == 1) {
                tradingHours[i] = new TradingHours(null, null, true);
            } else {
                try {
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd:HHmm");
                    ZoneId easternTimeZone = ZoneId.of("America/New_York");

                    // 解析日期时间字符串为 LocalDateTime
                    LocalDateTime startTime = LocalDateTime.parse(startAndEnd[0], formatter).atZone(easternTimeZone).toLocalDateTime();
                    LocalDateTime endTime = LocalDateTime.parse(startAndEnd[1], formatter).atZone(easternTimeZone).toLocalDateTime();

                    if (secType.equals("STK") || secType.equals("CFD")) {
                        LocalDateTime openMarketTime = startTime.withHour(9).withMinute(30);
                        if (startTime.isBefore(openMarketTime)) {
                            startTime = openMarketTime;
                        }

                        LocalDateTime closeMarketTime = startTime.withHour(16).withMinute(0);
                        if (endTime.isAfter(closeMarketTime)) {
                            endTime = closeMarketTime;
                        }
                    }
                    tradingHours[i] = new TradingHours(startTime, endTime, false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        return tradingHours;
    }

    public static boolean isCloseToClosing(String symbol, String secType, LocalDateTime currentTime, int seconds) {
        boolean ret = false;
        String redisKey = String.format("%s:%s:TRADING_PERIODS", symbol, secType);
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String tradingHoursStr = jedis.get(redisKey);
            if (tradingHoursStr != null) {
                TradingHours[] tradingHours = parseTradingHours(tradingHoursStr, secType);
                for (TradingHours tradingHour : tradingHours) {
                    if (tradingHour.isClosed()) {
                        continue;
                    }

                    if (currentTime.isBefore(tradingHour.getEndTime()) && currentTime.plusSeconds(seconds).isAfter(tradingHour.getEndTime())) {
                        return true;
                    }
                }
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static boolean isMarketClose(String symbol, String secType, LocalDateTime currentTime) {
        boolean ret = false;
        String redisKey = String.format("%s:%s:TRADING_PERIODS", symbol, secType);
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String tradingHoursStr = jedis.get(redisKey);
            if (tradingHoursStr != null) {
                TradingHours[] tradingHours = parseTradingHours(tradingHoursStr, secType);
                for (TradingHours tradingHour : tradingHours) {
                    if (tradingHour.isClosed()) {
                        return true;
                    }

                    if (currentTime.isAfter(tradingHour.getEndTime()) && currentTime.isBefore(tradingHour.getStartTime())) {
                        return true;
                    }
                }
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ret;
    }

    public static String genKey(String symbol, String secType) {
        return symbol + ":" + secType;
    }

    public static String[] parseKey(String key) {
        return key.split(":");
    }

    public static DoubleColumn ewm(DoubleColumn inputCol, double alpha, String outputColumnName, boolean prefillSma, boolean adjust, int period, int minPeriods) {
        DoubleColumn result = DoubleColumn.create(outputColumnName, inputCol.size());
        int startIndex = 1;
        if (prefillSma) {
            DoubleColumn sma = inputCol.rolling(period).mean();
            Double initialSma = sma.getDouble(period - 1);
            if (initialSma == null || initialSma.isNaN()) {
                initialSma = 0.0d;
            }
            result.set(period - 1, initialSma);
            startIndex = period;
        } else {
            result.set(0, inputCol.getDouble(0));
        }
        if (!adjust) {
            for (int i = startIndex; i < inputCol.size(); i++) {
                Double prevValue = result.getDouble(i - 1);
                if (prevValue == null || prevValue.isNaN()) {
                    prevValue = 0.0d;
                }
                double ema = inputCol.getDouble(i) * alpha + prevValue * (1 - alpha);
                result.set(i, ema);
            }
        } else {
            double alphaWeightedSum = 0;
            double alphaWeightedInputSum = 0;

            for (int i = 0; i < inputCol.size(); i++) {
                double alphaWeightRet = Math.pow(1 - alpha, i);
                alphaWeightedSum += alphaWeightRet;

                alphaWeightedInputSum = (alphaWeightedInputSum * (1 - alpha) + inputCol.getDouble(i));

                if (alphaWeightedSum != 0) {
                    result.set(i, alphaWeightedInputSum / alphaWeightedSum);
                } else {
                    result.set(i, 0);
                }
            }
        }

        if (!prefillSma || adjust) {
            for (int i = 0; i <= minPeriods; i++) {
                result.set(i, DoubleColumnType.missingValueIndicator());
            }
        }
        return result;
    }

    public static double roundNum(double num, int decimals) {
        BigDecimal bd = new BigDecimal(num);
        bd = bd.setScale(decimals, RoundingMode.HALF_UP);

        return bd.doubleValue();
    }

    public static void clearLastActionInfo(String redisKey) {
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ActionInfo getLastActionInfo(String redisKey) {
        ActionInfo actionInfo = new ActionInfo();
        actionInfo.setAction(Signal.TradeActionType.NO_ACTION);
        actionInfo.setDateTime(null);

        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String storedValue = jedis.get(redisKey);
            if (storedValue == null) {
                return actionInfo;
            }

            String[] split = storedValue.split("\\|");
            String action = split[0];
            String datetime = split[1];
            switch (action) {
                case "NO_ACTION":
                    break;
                case "BUY":
                    actionInfo.setAction(Signal.TradeActionType.BUY);
                    actionInfo.setDateTime(Utils.genUsDateTime(datetime, "yyyy-MM-dd HH:mm:ssXXX"));
                    break;
                case "SELL":
                    actionInfo.setAction(Signal.TradeActionType.SELL);
                    actionInfo.setDateTime(Utils.genUsDateTime(datetime, "yyyy-MM-dd HH:mm:ssXXX"));
                    break;
            }
            return actionInfo;

        } catch (Exception e) {
            e.printStackTrace();
            return actionInfo;
        }
    }

    public static boolean isInProgress(String redisKey) {
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(redisKey);
            if (value == null) {
                return false;
            }
            return value.equals("true");

        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void setInProgress(String redisKey) {
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(redisKey, "true");
            long timestamp = System.currentTimeMillis() / 1000 + 60;
            jedis.expireAt(redisKey, timestamp);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static LocalDateTime genUsDateTime(String dateTimeStr, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        ZoneId easternTimeZone = ZoneId.of("America/New_York");
        try {
            // 解析日期时间字符串并获取Date对象
            return LocalDateTime.parse(dateTimeStr, formatter).atZone(easternTimeZone).toLocalDateTime();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static LocalDateTime genUsDateTimeNow() {
        ZonedDateTime easternTime = ZonedDateTime.now(ZoneId.of("America/New_York"));
        return easternTime.toLocalDateTime();
    }

    public static double[] calculateSignalMargin(String secType, float signalMargin, float signalMarginOffset, double volatilityMultiplier, int position) {
        if (!secType.equals("FUT")) {
            position = position / 100;
        }

        double buySignalMargin = (signalMargin + signalMarginOffset * position) * volatilityMultiplier;
        double sellSignalMargin = (signalMargin - signalMarginOffset * position) * volatilityMultiplier;
        return new double[]{buySignalMargin, sellSignalMargin};
    }

    public static Signal fulfillSignal(String accountId, String symbol, String secType, double wap, int position, Signal.OrderType orderType, String benchmarkColumnName) {
        Signal signal = new Signal();
        signal.setValid(true);
        signal.setAccountId(accountId);
        signal.setUuid(UUID.randomUUID().toString());
        signal.setSymbol(symbol);
        signal.setSecType(secType);
        signal.setWap(wap);
        signal.setQuantity(position);
        signal.setOrderType(orderType);
        signal.setBenchmarkColumn(benchmarkColumnName);
        return signal;
    }

    public static List<OrderInfo> getOrderList(Jedis jedis, String accountId, String symbol, String secType) {
        try {
            String redisKey = accountId + ":" + symbol + ":" + secType + ":ORDER_LIST";
            String storedOrderListJson = jedis.get(redisKey);
            logger.debug("redis|" + redisKey + "|" + storedOrderListJson);
            if (storedOrderListJson == null) {
                return null;
            }

            ObjectMapper objectMapper = new ObjectMapper();
            List<OrderInfo> orderList = objectMapper.readValue(storedOrderListJson, new TypeReference<>() {
            });
            if (orderList == null && orderList.size() == 0) {
                return null;
            }
            return orderList;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static double calToVolatilityMultiplier(AppConfigManager.AppConfig.AlgorithmConfig ac, double volatility) {
        return ac.getVolatilityA() +
                ac.getVolatilityB() * volatility +
                ac.getVolatilityC() * volatility * volatility;
    }
}
