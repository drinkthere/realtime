package sma;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.algorithms.Sma;
import capital.daphne.algorithms.close.CloseAlgorithm;
import capital.daphne.algorithms.close.MACDSingal;
import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class SmaTest {

    private static final Logger logger = LoggerFactory.getLogger(SmaTest.class);

    private double preMaxWap = 0.0;
    private double preMinWap = 0.0;
    private double currMaxWap = 0.0;
    private double currMinWap = 0.0;

    private AppConfigManager.AppConfig.AlgorithmConfig ac;


    @Test
    public void testSma() {
        //!!!!!!!!!!!!!!!!!!!!!! 记得修改下MACDSignal/MACDZERO的计算时间的代码，把对应的行注释掉
        logger.info("initialize cache handler: redis");
        JedisManager.initializeJedisPool();

        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        List<AppConfigManager.AppConfig.AlgorithmConfig> algorithms = appConfig.getAlgorithms();
        ac = algorithms.get(0);
        Sma sma = new Sma(ac);
        CloseAlgorithm closeProcessor = new MACDSingal(ac);

        String currentDirectory = System.getProperty("user.dir");

        // 读取csv文件，加载List<Bar>
        List<Bar> bars = loadCsv(currentDirectory + "/src/test/java/sma/spy_2023-09-27.csv");

        // 生成我们需要的barList
        List<BarInfo> barList = new ArrayList<>();
        int maxBarListSize = 60;

        int position = 0;
        int maxPosition = ac.getMaxPortfolioPositions();

        List<Sig> result = new ArrayList<>();
        for (int i = 0; i < bars.size(); i++) {
            Bar bar = bars.get(i);

            BarInfo barInfo = processBar(bar);
            if (barInfo == null) {
                continue;
            }

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

    private List<Bar> loadCsv(String csvFilePath) {
        List<Bar> bars = new ArrayList<>();

        try (FileReader fileReader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord csvRecord : csvParser) {
                Bar bar = new Bar();
                bar.setDate(csvRecord.get("date"));
                bar.setOpen(Double.parseDouble(csvRecord.get("open")));
                bar.setHigh(Double.parseDouble(csvRecord.get("high")));
                bar.setLow(Double.parseDouble(csvRecord.get("low")));
                bar.setClose(Double.parseDouble(csvRecord.get("close")));
                bar.setVolume(Double.parseDouble(csvRecord.get("volume")));
                bar.setVwap(Double.parseDouble(csvRecord.get("average")));
                bar.setBarCount(Integer.parseInt(csvRecord.get("barCount")));
                bars.add(bar);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bars;
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

        if (wap > currMaxWap) {
            currMaxWap = wap;
        }
        if (currMinWap == 0 || wap < currMinWap) {
            currMinWap = wap;
        }

        try {
            BarInfo barInfo = new BarInfo();
            barInfo.setDate(bar.getDate());
            barInfo.setVwap(wap);
            double[] wapArr = new double[]{preMaxWap, preMinWap, currMaxWap, currMinWap};
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

        if (prevMinWap == 0) {
            prevMinWap = currMinWap;
        }

        double maxWap = Math.max(prevMaxWap, currMaxWap);
        double minWap = Math.min(prevMinWap, currMinWap);
        double volatility = (maxWap - minWap) / minWap;
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

@Data
class Bar {
    private String date;
    private double open;
    private double high;
    private double low;
    private double close;
    private double volume;
    private double vwap;
    private int barCount;
}

@Data
class BarInfo {
    private String date;
    private double vwap;
    private double volatility;
}

@Data
class Sig {
    private int index;
    private int quantity;
    private String orderType;
}
