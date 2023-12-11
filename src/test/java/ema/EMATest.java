package ema;

import capital.daphne.AppConfigManager;
import capital.daphne.DbManager;
import capital.daphne.JedisManager;
import capital.daphne.algorithms.AlgorithmProcessor;
import capital.daphne.algorithms.EMA;
import capital.daphne.algorithms.close.TrailingStop;
import capital.daphne.models.BarInfo;
import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import capital.daphne.models.WapCache;
import capital.daphne.services.BarSvc;
import capital.daphne.utils.Utils;
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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

public class EMATest {

    private static final Logger logger = LoggerFactory.getLogger(EMATest.class);

    private AppConfigManager.AppConfig.AlgorithmConfig ac;

    private WapCache wapMaxMin;

    private BarSvc barsvc;

    private String key;

    private List<String> wapList;

    @Test
    public void testSma() {
        logger.info("initialize database handler: mysql");
        DbManager.initializeDbConnectionPool();

        barsvc = new BarSvc();
        //!!!!!!!!!!!!!!!!!!!!!! 记得修改下MACDSignal/MACDZERO的计算时间的代码，把对应的行注释掉
        logger.info("initialize cache handler: redis");
        JedisManager.initializeJedisPool();

        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        List<AppConfigManager.AppConfig.AlgorithmConfig> algorithms = appConfig.getAlgorithms();
        ac = algorithms.get(0);
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();
        key = symbol + ":" + secType;

        EMA ema = new EMA(ac);
        AlgorithmProcessor closeProcessor = new TrailingStop(ac);

        String currentDirectory = System.getProperty("user.dir");

        wapMaxMin = new WapCache();

        // 读取csv文件，加载List<Bar>
        List<Bar> bars = TestUtils.loadCsv(currentDirectory + "/src/test/java/sma/SPY_20231208.csv");

        // 生成我们需要的barList
        List<BarInfo> barList = new ArrayList<>();
        int maxBarListSize = ac.getNumStatsBars() + 1;

        int position = 0;
        int maxPosition = ac.getMaxPortfolioPositions();

        String day = null;
        List<Sig> result = new ArrayList<>();
        wapList = new ArrayList<>();
        boolean isFirst = true;
        for (int i = 0; i < bars.size(); i++) {
            Bar bar = bars.get(i);
            String dateStr = bar.getDate();
            String[] splits = dateStr.split(" ");
            String processDate = splits[0];
            if (day == null || !day.equals(processDate)) {
                day = processDate;
                wapList.clear();
                barsvc.clearEma(ac.getAccountId() + ":" + ac.getSymbol() + ":" + ac.getSecType());
            }

            BarInfo barInfo = processBar(bar, ac);
            if (barInfo == null) {
                continue;
            }
            barList.add(barInfo);
            if (barList.size() > maxBarListSize) {
                // 如果超过最大值，移除最早加入barList的数据
                barList.remove(0);
            }

            if (barList.size() < maxBarListSize) {
                continue;
            }

            // 初始化ema，内部判断，只进行一次
            barsvc.initEma(ac.getAccountId() + ":" + ac.getSymbol() + ":" + ac.getSecType(), wapList, ac.getNumStatsBars());

            // 生成dataframe
            Table df = getTable(barList);
            Row row = df.row(df.rowCount() - 1);
            Signal signal = ema.getSignal(df, position, maxPosition, row.getDouble("vwap"), row.getDouble("vwap"));

            String nowDateTime = row.getString("date_us");
            LocalDateTime localDateTime = Utils.genUsDateTime(nowDateTime, "yyyy-MM-dd HH:mm:ssXXX");
            int hour = localDateTime.getHour();
            int minute = localDateTime.getMinute();
            if (!isTradingNow(hour, minute)) {
                continue;
            }

            if (signal != null && signal.isValid()) {
                position += signal.getQuantity();
                Sig sig = new Sig();
                sig.setIndex(i);
                sig.setQuantity(signal.getQuantity());
                sig.setOrderType("OPEN");
                sig.setDatetime(bar.getDate());
                result.add(sig);

                // update order list in redis
                updateOrdersInRedis(i, signal.getQuantity(), "OPEN", row);
                wapMaxMin.setMaxWap(signal.getWap());
                wapMaxMin.setMinWap(signal.getWap());
            } else {
                if (ac.getCloseAlgo() != null && df.rowCount() == maxBarListSize) {
                    signal = closeProcessor.getSignal(df, position, maxPosition, row.getDouble("vwap"), row.getDouble("vwap"));
                    if (signal != null && signal.isValid()) {
                        position += signal.getQuantity();
                        Sig sig = new Sig();
                        sig.setIndex(i);
                        sig.setQuantity(signal.getQuantity());
                        sig.setOrderType("CLOSE");
                        sig.setDatetime(bar.getDate());
                        result.add(sig);
                        // update order list in redis
                        updateOrdersInRedis(i, signal.getQuantity(), "CLOSE", row);
                    }
                }
            }
        }

        for (Sig sig : result) {
            System.out.println(sig);
        }
        System.out.println("Order num:" + result.size());
    }

    private BarInfo processBar(Bar bar, AppConfigManager.AppConfig.AlgorithmConfig ac) {
        // 获取wap信息
        double wap = bar.getVwap();
        // 如果volume <= 0 （MIDPOINT 或 部分TRADES), 这个时候用TWAP;
        if (bar.getVolume() <= 0) {
            wap = (bar.getOpen() + bar.getHigh() + bar.getLow() + bar.getClose()) / 4;
        }

        // datasource 相关配置
        int historyDurationSeconds = 7200;
        // 只保留指定数量的wapList
        int maxKeepNumOfWap = historyDurationSeconds / 5;
        if (wapList.size() >= maxKeepNumOfWap) {
            // 如果长度超过maxLength，从队头移除一项
            wapList.remove(0);
        }
        // 添加新数据到队尾
        wapList.add(String.valueOf(bar.getVwap()));

        if (wapList.size() < maxKeepNumOfWap) {
            // 数量不够，先返回
            return null;
        }
        // 更新订单之间的wapMaxMin
        boolean dataUpdate = false;
        if (wap >= wapMaxMin.getMaxWap()) {
            wapMaxMin.setMaxWap(wap);
            dataUpdate = true;
        }
        if (wap <= wapMaxMin.getMinWap()) {
            wapMaxMin.setMinWap(wap);
            dataUpdate = true;
        }
        logger.debug(String.format("update wapMaxMin, max=%f, min=%f, wap=%f", wapMaxMin.getMaxWap(), wapMaxMin.getMinWap(), wap));
        if (dataUpdate) {
            TestUtils.updateWapMaxMinInRedis(key, wapMaxMin);
        }


        BarInfo barInfo = new BarInfo();
        try {
            barInfo.setDate(bar.getDate());
            barInfo.setVwap(wap);
            barInfo.setHigh(bar.getHigh());
            barInfo.setLow(bar.getLow());
            barInfo.setOpen(bar.getOpen());
            barInfo.setClose(bar.getClose());
            double volatility = TestUtils.calVolatility(ac, wapList.subList(0, wapList.size() - 1), bar.getDate());
            barInfo.setVolatility(volatility);
            return barInfo;
        } catch (Exception e) {
            return null;
        }
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
//        dataframe.removeColumns("vwap");
//        dataframe.addColumns(prevVWapColumn.setName("vwap"));
//
//        prevVWapColumn = dataframe.doubleColumn("vwap").lag(1);
        dataframe.addColumns(prevVWapColumn.setName("prev_vwap"));
        return dataframe;
    }

    private void updateOrdersInRedis(int orderId, int quantity, String orderType, Row row) {
        String dateNow = row.getString("date_us");
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = String.format("%s:%s:%s:ORDER_LIST", ac.getAccountId(), ac.getSymbol(), ac.getSecType());
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

    private boolean isTradingNow(int hour, int minute) {
        if (hour == 9 && minute >= 30) {
            return true;
        } else if (hour > 9 && hour < 16) {
            return true;
        } else if (hour == 16 && minute == 0) {
            return true;
        }
        return false;
    }

    @Test
    public void testGenMultiplier() {
        double volatilityMultiplier = Utils.calToVolatilityMultiplier(0.2, 200, -250, 0.001213);
        System.out.println(volatilityMultiplier);
    }

}
