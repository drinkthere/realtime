package capital.daphne.utils;

import capital.daphne.JedisManager;
import capital.daphne.models.ActionInfo;
import capital.daphne.models.Signal;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.columns.numbers.DoubleColumnType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.TimeZone;

public class Utils {
    public static boolean isMarketOpen() {
        Calendar calendar = Calendar.getInstance();

        // 设置美东时区
        TimeZone timeZone = TimeZone.getTimeZone("America/New_York"); // 美东时区
        calendar.setTimeZone(timeZone);

        // 获取当前日期和时间信息
        int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
        int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);

        // 判断是否在周一到周五的早上9:30到下午4:00之间
        if (dayOfWeek >= Calendar.MONDAY && dayOfWeek <= Calendar.FRIDAY) {
            if (hourOfDay == 9 && minute >= 30) {
                return true;
            } else if (hourOfDay > 9 && hourOfDay < 16) {
                return true;
            } else if (hourOfDay == 16 && minute == 0) {
                return true;
            }
        }

        return false;
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

    public static LocalDateTime loadUsDateTime(String datetime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(datetime, formatter);
        return zonedDateTime.toLocalDateTime();
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
                    actionInfo.setDateTime(Utils.loadUsDateTime(datetime));
                    break;
                case "SELL":
                    actionInfo.setAction(Signal.TradeActionType.SELL);
                    actionInfo.setDateTime(Utils.loadUsDateTime(datetime));
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
}
