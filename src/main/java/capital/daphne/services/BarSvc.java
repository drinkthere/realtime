package capital.daphne.services;

import capital.daphne.AppConfigManager;
import capital.daphne.DbManager;
import capital.daphne.JedisManager;
import capital.daphne.models.BarInfo;
import capital.daphne.utils.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BarSvc {
    private static final Logger logger = LoggerFactory.getLogger(BarSvc.class);

    private Map<String, Double> openMarketVolatilityFactorMap;

    private Map<String, String> wapKeyMap;

    public BarSvc() {
        openMarketVolatilityFactorMap = new HashMap<>();
    }

    /**
     * 通过datasource服务获取BAR_LIST写入Redis，这里从Redis读取数据
     * 如果还没有达到所需的最少bar数量，就返回null，避免判断是否要给出signal的时候因为数据缺失出问题
     * 返回数据前，需要计算出prev_vwap
     */
    public Table getDataTable(String key, AppConfigManager.AppConfig.AlgorithmConfig ac, List<String> wapList) {
        int minBarNum = ac.getNumStatsBars();
        JedisPool jedisPool = JedisManager.getJedisPool();

        try (Jedis jedis = jedisPool.getResource()) {
            // 数据只与symbol和secType油管，和accountId无关
            String redisKey = key + ":BAR_LIST";
            String storedBarListJson = jedis.get(redisKey);
            if (storedBarListJson != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                List<BarInfo> barList = objectMapper.readValue(storedBarListJson, new TypeReference<>() {
                });

                if (barList.size() < minBarNum) {
                    logger.info(String.format("barList is not ready, minBarNum=%d, currBarNum=%d", minBarNum, barList.size()));
                    return null;
                }

                Table dataframe = Table.create("IBKR Bar Dataframe");
                // 添加列到表格
                dataframe.addColumns(
                        StringColumn.create("date_us"),
                        DoubleColumn.create("vwap"),
                        DoubleColumn.create("open"),
                        DoubleColumn.create("close"),
                        DoubleColumn.create("high"),
                        DoubleColumn.create("low"),
                        DoubleColumn.create("volatility"));

                // for prev_wap, so add 1
                List<BarInfo> validBarList = barList.subList(barList.size() - (minBarNum + 1), barList.size());

                for (BarInfo bar : validBarList) {
                    dataframe.stringColumn("date_us").append(bar.getDate());
                    dataframe.doubleColumn("vwap").append(bar.getVwap());
                    dataframe.doubleColumn("open").append(bar.getOpen());
                    dataframe.doubleColumn("high").append(bar.getHigh());
                    dataframe.doubleColumn("low").append(bar.getLow());
                    dataframe.doubleColumn("close").append(bar.getClose());
                    double volatility = calVolatility(ac, wapList);
                    dataframe.doubleColumn("volatility").append(volatility);
                }

                DoubleColumn prevVWapColumn = dataframe.doubleColumn("vwap").lag(1);
                dataframe.addColumns(prevVWapColumn.setName("prev_vwap"));
                return dataframe;
            } else {
                logger.info(key + " barList is empty");
                return null;
            }
        } catch (Exception e) {
            logger.error("getDataTable, error:" + e.getMessage());
            return null;
        }
    }

    public List<String> getWapList(String key) {
        // 因为存在parallel和rewrite的symbol，所以先做key映射然后在获取对应的wap信息
        List<String> wapList = new ArrayList<>();
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String fromKey = String.format("%s:KEY_MAP", key);
            String toKey = jedis.get(fromKey);
            if (toKey == null) {
                return wapList;
            }

            String redisKey = String.format("%s:WAP_LIST", toKey);
            List<String> storedList = jedis.lrange(redisKey, 0, -1);
            return storedList == null ? wapList : storedList;
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(key + " get wapList failed, error:" + e.getMessage());
            return wapList;
        }
    }

    public double calVolatility(AppConfigManager.AppConfig.AlgorithmConfig ac, List<String> wapList) {
        double max = 0.0;
        double min = 0.0;

        double totalWeight = 0.0;
        double totalVolatility = 0.0;
        // 将wapList中的数据分成10份
        for (int i = 0; i < 10; i++) {
            int startPercentage = i * 10;
            int startIndex = (int) (wapList.size() * (startPercentage / 100.0));
            // 获取对应百分比的subset
            List<String> subset = wapList.subList(startIndex, wapList.size());
            double[] maxMin = calMaxMin(subset);
            if (i == 0) {
                // 全集数据计算出来的max, min和更新到数据库中
                max = maxMin[0];
                min = maxMin[1];
            }
            // 计算subset的volatility
            double subsetVolatility = (maxMin[0] - maxMin[1]) / maxMin[1];
            // 然后式用a + bx + c^2计算出不同时间段的权重
            double timeWeight = calTimeWeighted(i + 1, ac);
            logger.debug(String.format("subset size=%d, timeWeight=%f, subsetVola=%f", subset.size(), timeWeight, subsetVolatility));
            totalWeight += timeWeight;
            totalVolatility += subsetVolatility * timeWeight;
        }

        // 计算volatility的加权平均值
        double volatility = totalVolatility / totalWeight;
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();

        double adjustedVolatility = adjustVolatilityDuringMarketOpenPeriod(volatility, ac);

        // 更新max和min
        if (max > 0 && min > 0 && volatility > 0 && adjustedVolatility > 0) {
            logMaxMinWap(ac.getAccountId(), symbol, secType, max, min, volatility, adjustedVolatility);
        } else {
            logger.warn(String.format("max=%f, min=%f, volatility=%f, adjustedVolatility=%f", max, min, volatility, adjustedVolatility));
        }
        return adjustedVolatility;
    }

    private double[] calMaxMin(List<String> wapList) {
        // 计算wapList的max和min
        double newMax = Double.MIN_VALUE;
        double newMin = Double.MAX_VALUE;
        for (String wapStr : wapList) {
            double wap = Double.parseDouble(wapStr);
            newMax = Math.max(newMax, wap);
            newMin = Math.min(newMin, wap);
        }
        return new double[]{newMax, newMin};
    }

    private double calTimeWeighted(int i, AppConfigManager.AppConfig.AlgorithmConfig ac) {
        return ac.getTimeWeightedA() + ac.getTimeWeightedB() * i + ac.getTimeWeightedC() * i * i;
    }

    public void logMaxMinWap(String accountId, String symbol, String secType, double maxWap, double minWap, double volatility, double adjustedVolatility) {
        try (Connection connection = DbManager.getConnection()) {
            String insertSQL = "INSERT INTO tb_wap_log (account_id, symbol, sec_type, max_wap, min_wap, volatility, adjusted_volatility) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement insertStatement = connection.prepareStatement(insertSQL);
            insertStatement.setString(1, accountId);
            insertStatement.setString(2, symbol);
            insertStatement.setString(3, secType);
            insertStatement.setDouble(4, maxWap);
            insertStatement.setDouble(5, minWap);
            insertStatement.setDouble(6, volatility);
            insertStatement.setDouble(7, adjustedVolatility);
            int rowCount = insertStatement.executeUpdate();
            if (rowCount > 0) {
                logger.debug(String.format("%s %s %s max_wap=%f, min_wap=%f, volatility=%f adjustedVolatility=%f insert wap mark successfully.",
                        accountId, symbol, secType, maxWap, minWap, volatility, adjustedVolatility));
            } else {
                logger.error(String.format("%s %s %s max_wap=%f, min_wap=%f, volatility=%f adjustedVolatility=%f insert wap mark failed.",
                        accountId, symbol, secType, maxWap, minWap, volatility, adjustedVolatility));
            }

        } catch (SQLException e) {
            e.printStackTrace();
            logger.error(String.format("%s %s %s max_wap=%f, min_wap=%f volatility=%f adjustedVolatility=%f save wap mark failed.",
                    accountId, symbol, secType, maxWap, minWap, volatility, adjustedVolatility));
        }
    }

    public double adjustVolatilityDuringMarketOpenPeriod(double volatility, AppConfigManager.AppConfig.AlgorithmConfig ac) {
        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();
        String key = String.format("%s:%s:%s", accountId, symbol, secType);

        LocalDateTime marketOpenTime = Utils.getMarketOpenTime(symbol, secType);
        if (marketOpenTime == null) {
            logger.error(String.format("%s %s market open time is null", symbol, secType));
            return volatility;
        }

        LocalDateTime now = Utils.genUsDateTimeNow();
        Duration duration = Duration.between(marketOpenTime, now);

        // 判断当前时间是否在递减volatility周期内
        if (duration.getSeconds() < 0) {
            // 如果是盘前，不对volatility进行调整
            return volatility;
        } else if (duration.getSeconds() >= 0 && duration.getSeconds() < 5) {
            // 开盘的bar
            double openMarketVolatilityFactor = calOpenMarketVolatilityFactor(volatility, ac);
            openMarketVolatilityFactorMap.put(key, openMarketVolatilityFactor);
            volatility = openMarketVolatilityFactor * volatility;
            return volatility;
        } else if (duration.getSeconds() <= ac.getMarketOpenReductionSeconds()) {
            double openMarketVolatilityFactor = openMarketVolatilityFactorMap.get(key);
            double currOpenMarketVolatilityFactor = calCurrentVolatilityFactor(openMarketVolatilityFactor, duration.getSeconds(), ac.getMarketOpenReductionSeconds(), ac.getReductionFactor());
            volatility = currOpenMarketVolatilityFactor * volatility;
            return volatility;
        } else {
            // 过了衰减期，不对volatility进行调整
            return volatility;
        }
    }

    private double calOpenMarketVolatilityFactor(double volatility, AppConfigManager.AppConfig.AlgorithmConfig ac) {
        return ac.getVolatilityOpenMarketK() / (volatility * volatility) + 1;
    }

    private double calCurrentVolatilityFactor(double openMarketVolatilityFactor, long passedSeconds, int totalSeconds, double k) {
        if (openMarketVolatilityFactor < 1) {
            // 起始波动率不能小于1
            return 1;
        }

        if (k == 0) {
            return openMarketVolatilityFactor - (openMarketVolatilityFactor - 1) * passedSeconds / totalSeconds;
        } else {

            double r = (openMarketVolatilityFactor - 1) * Math.exp(-k * passedSeconds) + 1;
            // 老公式
            //double r = openMarketVolatilityFactor * (1 - Math.pow( 1 - 1/openMarketVolatilityFactor, Math.pow((float)passedSeconds/totalSeconds, -k)));
            return r;
        }
    }

    public void initEma(String accountId, String symbol, String secType, List<String> wapList, int numStatsBars) {
        double ema = getEma(accountId, symbol, secType);
        if (ema > 0.0) {
            return;
        }

        // 生成ema
        DoubleColumn wapColumn = DoubleColumn.create("wapCol");
        for (String wapStr : wapList) {
            double wap = Double.parseDouble(wapStr);
            wapColumn.append(wap);
        }
        int period = numStatsBars;
        double multiplier = 2.0 / (period + 1);
        // 原始公式用的是prev_vwap来计算的，所以需要lag(1), 但是这里我们希望计算的是prev_ema，所以lag(2)。后面我们还会用这里的prev_ema计算当前bar的ema。
        DoubleColumn prevEmaColumn = Utils.ewm(wapColumn.lag(2), multiplier, "prevEma", true, false, period, period - 1);
        Double prevEmaValue = prevEmaColumn.get(prevEmaColumn.size() - 1);
        setEma(accountId, symbol, secType, prevEmaValue);
    }

    public void setEma(String accountId, String symbol, String secType, double ema) {
        String redisKey = String.format("%s:%s:%s:EMA", accountId, symbol, secType);
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.set(redisKey, String.valueOf(ema));
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(redisKey + " set ema failed, error:" + e.getMessage());
        }
    }

    public Double getEma(String accountId, String symbol, String secType) {
        String redisKey = String.format("%s:%s:%s:EMA", accountId, symbol, secType);
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String emaString = jedis.get(redisKey);
            return emaString == null ? 0.0 : Double.parseDouble(emaString);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(redisKey + " get ema failed, error:" + e.getMessage());
            return 0.0;
        }
    }


    public void clearEma(String key) {
        String redisKey = String.format("%s:EMA", key);
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(redisKey + " clear ema failed, error:" + e.getMessage());
        }
    }
}
