package sma;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.algorithms.AlgorithmProcessor;
import capital.daphne.algorithms.SMA;
import capital.daphne.algorithms.close.MACDSingal;
import capital.daphne.models.BarInfo;
import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import testmodels.Bar;
import testmodels.Sig;
import testutils.TestUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SMATest {

    private static final Logger logger = LoggerFactory.getLogger(SMATest.class);

    private AppConfigManager.AppConfig.AlgorithmConfig ac;

    private List<String> dateList;
    private Map<String, Double> maxWapMap;
    private Map<String, Double> minWapMap;

    @Test
    public void testSma() {
        //!!!!!!!!!!!!!!!!!!!!!! 记得修改下MACDSignal/MACDZERO的计算时间的代码，把对应的行注释掉
        logger.info("initialize cache handler: redis");
        JedisManager.initializeJedisPool();

        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        List<AppConfigManager.AppConfig.AlgorithmConfig> algorithms = appConfig.getAlgorithms();
        ac = algorithms.get(0);
        SMA sma = new SMA(ac);
        AlgorithmProcessor closeProcessor = new MACDSingal(ac);

        String currentDirectory = System.getProperty("user.dir");

        dateList = new ArrayList<>();
        maxWapMap = new HashMap<>();
        minWapMap = new HashMap<>();

        // 读取csv文件，加载List<Bar>
        List<Bar> bars = TestUtils.loadCsv(currentDirectory + "/src/test/java/sma/spy_2023-09-27--2023-09-29.csv");

        // 生成我们需要的barList
        List<BarInfo> barList = new ArrayList<>();
        int maxBarListSize = 1200;

        int position = 0;
        int maxPosition = ac.getMaxPortfolioPositions();

        List<Sig> result = new ArrayList<>();
        for (int i = 0; i < bars.size(); i++) {
            Bar bar = bars.get(i);

            BarInfo barInfo = processBar(bar);
            if (barInfo == null) {
                continue;
            }
            // System.out.println(barInfo.getVwap());
            barList.add(barInfo);
            if (barList.size() > maxBarListSize) {
                // 如果超过最大值，移除最早加入barList的数据
                barList.remove(0);
            }

            if (barList.size() < ac.getNumStatsBars()) {
                continue;
            }

            // 生成dataframe
            Table df = getTable(barList);
            Row row = df.row(df.rowCount() - 1);

            // 获取信号
            Signal signal = sma.getSignal(df, position, maxPosition);
            if (signal != null && signal.isValid()) {
                position += signal.getQuantity();
                Sig sig = new Sig();
                sig.setIndex(i);
                sig.setQuantity(signal.getQuantity());
                sig.setOrderType("OPEN");
                result.add(sig);

                // update order list in redis
                updateOrdersInRedis(ac.getSymbol(), ac.getSecType(), i, signal.getQuantity(), "OPEN", row);
            } else {
                if (ac.getCloseAlgo() != null && df.rowCount() == maxBarListSize) {
                    signal = closeProcessor.getSignal(df, position, maxPosition);
                    if (signal != null && signal.isValid()) {
                        position += signal.getQuantity();
                        Sig sig = new Sig();
                        sig.setIndex(i);
                        sig.setQuantity(signal.getQuantity());
                        sig.setOrderType("CLOSE");
                        result.add(sig);
                        // update order list in redis
                        updateOrdersInRedis(ac.getSymbol(), ac.getSecType(), i, signal.getQuantity(), "CLOSE", row);
                    }
                }
            }
        }

        for (Sig sig : result) {
            System.out.println(sig);
        }
        System.out.println("Order num:" + result.size());
    }

    private BarInfo processBar(Bar bar) {
        double vwap = bar.getVwap();
        double wap;
        if (bar.getVolume() <= 0) {
            wap = (bar.getOpen() + bar.getHigh() + bar.getLow() + bar.getClose()) / 4;
            if (wap / vwap >= 1.001 || wap / vwap <= 0.999) {
                return null;
            }
        } else {
            wap = vwap;
        }

        String[] s = bar.getDate().split(" ");
        String dateStr = s[0];
        double prevMax;
        double prevMin;
        double currMax;
        double currMin;
        if (maxWapMap.containsKey(dateStr)) {
            currMax = maxWapMap.get(dateStr);
        } else {
            currMax = wap;
            maxWapMap.put(dateStr, wap);
            dateList.add(dateStr);
        }

        if (minWapMap.containsKey(dateStr)) {
            currMin = minWapMap.get(dateStr);
        } else {
            currMin = wap;
            minWapMap.put(dateStr, wap);
            dateList.add(dateStr);
        }

        boolean needUpdated = false;
        if (wap > currMax) {
            currMax = wap;
            needUpdated = true;
        }
        if (wap < currMin) {
            currMin = wap;
            needUpdated = true;
        }

        if (needUpdated) {
            minWapMap.put(dateStr, currMin);
            maxWapMap.put(dateStr, currMax);
        }

        int i = dateList.indexOf(dateStr);
        if (i == -1) {
            prevMax = 0.0;
            prevMin = 0.0;
            logger.info("error date index");
            System.exit(-1);
        } else if (i == 0) {
            prevMax = 0.0;
            prevMin = 0.0;
        } else {
            String prevDateStr = dateList.get(i - 1);
            prevMax = maxWapMap.getOrDefault(prevDateStr, 0.0);
            prevMin = minWapMap.getOrDefault(prevDateStr, 0.0);
            if (prevMax == 0.0 || prevMin == 0.0) {
                logger.info("error date index");
                System.exit(-1);
            }
        }


        try {
            BarInfo barInfo = new BarInfo();
            barInfo.setDate(bar.getDate());
            barInfo.setVwap(wap);
            double[] wapArr = new double[]{prevMax, prevMin, currMax, currMin};
            double volatility = calVolatility(wap, wapArr);

            barInfo.setVolatility(volatility);
            return barInfo;
        } catch (Exception e) {
            return null;
        }
    }

    private double calVolatility(double wap, double[] wapArr) {
        double prevMaxWap = wapArr[0];
        double prevMinWap = wapArr[1];
        double currMaxWap = wapArr[2];
        double currMinWap = wapArr[3];

        if (prevMaxWap == 0) {
            prevMaxWap = currMaxWap;
        }

        if (prevMinWap == 0) {
            prevMinWap = currMinWap;
        }


        double maxWap = Math.max(prevMaxWap, currMaxWap);
        double minWap = Math.min(prevMinWap, currMinWap);
        double volatility = (maxWap - minWap) / minWap;
        // System.out.println(maxWap + "|" + minWap);
        return volatility;
        // return Utils.roundNum(volatility, 6);
    }

    private Table getTable(List<BarInfo> barList) {
        Table dataframe = Table.create("IBKR Bar Dataframe");
        // 添加列到表格
        dataframe.addColumns(
                StringColumn.create("date_us"),
                DoubleColumn.create("vwap"),
                DoubleColumn.create("volatility"));
        for (BarInfo barInfo : barList) {
            dataframe.stringColumn("date_us").append(barInfo.getDate());
            dataframe.doubleColumn("vwap").append(barInfo.getVwap());
            dataframe.doubleColumn("volatility").append(barInfo.getVolatility());
        }
        DoubleColumn prevVWapColumn = dataframe.doubleColumn("vwap").lag(1);
        dataframe.addColumns(prevVWapColumn.setName("prev_vwap"));
        return dataframe;
    }

    private void updateOrdersInRedis(String symbol, String secType, int orderId, int quantity, String orderType, Row row) {
        String dateNow = row.getString("date_us");
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = String.format("%s.%s.%s.ORDER_LIST", ac.getAccountId(), symbol, secType);
            if (orderType.equals("CLOSE")) {
                // close信号来时，清除队列中的数据
                jedis.del(redisKey);
            } else if (orderType.equals("OPEN")) {
                // open信号来时，添加数据进入队列
                List<OrderInfo> orderList = new ArrayList<>();
                String storedOrderListJson = jedis.get(redisKey);
                if (storedOrderListJson != null) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    orderList = objectMapper.readValue(storedOrderListJson, new TypeReference<>() {
                    });
                }
                // 添加新数据
                OrderInfo orderInfo = new OrderInfo();
                orderInfo.setOrderId(orderId);
                orderInfo.setQuantity(quantity);
                orderInfo.setDateTime(dateNow);
                orderList.add(orderInfo);
                ObjectMapper objectMapper = new ObjectMapper();
                String orderListJson = objectMapper.writeValueAsString(orderList);
                jedis.set(redisKey, orderListJson);

                long timestamp = System.currentTimeMillis() / 1000 + 300; // 300s 过期
                jedis.expireAt(redisKey, timestamp);
                logger.info(redisKey + " update order in redis successfully.");
            }
        } catch (Exception e) {
            logger.error("update position in redis failed, error:" + e.getMessage());
        }
    }
}
