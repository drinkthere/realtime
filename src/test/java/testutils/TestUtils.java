package testutils;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.models.BarInfo;
import capital.daphne.models.WapCache;
import capital.daphne.utils.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import testmodels.Bar;

import java.io.FileReader;
import java.time.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {
    private static double openMarketVolatilityFactor;
    private static Map<String, Double> openMarketVolatilityFactorMap = new HashMap<>();

    public static List<Bar> loadCsv(String csvFilePath) {
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

    public static Table getTable(List<BarInfo> barList, int minBarNum) {
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

        List<BarInfo> validBarList = barList.subList(barList.size() - minBarNum, barList.size());
        for (BarInfo bar : validBarList) {
            dataframe.stringColumn("date_us").append(bar.getDate());
            dataframe.doubleColumn("vwap").append(bar.getVwap());
            dataframe.doubleColumn("open").append(bar.getOpen());
            dataframe.doubleColumn("high").append(bar.getHigh());
            dataframe.doubleColumn("low").append(bar.getLow());
            dataframe.doubleColumn("close").append(bar.getClose());
            dataframe.doubleColumn("volatility").append(bar.getVolatility());
        }
        DoubleColumn prevVWapColumn = dataframe.doubleColumn("vwap").lag(1);
        dataframe.addColumns(prevVWapColumn.setName("prev_vwap"));
        return dataframe;
    }

    public static LocalDateTime genUsDateTimeNow() {
        ZonedDateTime easternTime = ZonedDateTime.now(ZoneId.of("America/New_York"));
        return easternTime.toLocalDateTime();
    }

    public static void updateWapMaxMinInRedis(String key, WapCache wapMaxMin) {
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            ObjectMapper objectMapper = new ObjectMapper();
            String wapMaxMinJson = objectMapper.writeValueAsString(wapMaxMin);
            String redisKey = String.format("%s:MAX_MIN_WAP", key);
            jedis.set(redisKey, wapMaxMinJson);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static double calVolatility(AppConfigManager.AppConfig.AlgorithmConfig ac, List<String> wapList, String datetime) {
        double totalWeight = 0.0;
        double totalVolatility = 0.0;
        // 将wapList中的数据分成10份
        for (int i = 0; i < 10; i++) {
            int startPercentage = i * 10;
            int startIndex = (int) (wapList.size() * (startPercentage / 100.0));
            // 获取对应百分比的subset
            List<String> subset = wapList.subList(startIndex, wapList.size());
            double[] maxMin = calMaxMin(subset);
            // 计算subset的volatility
            double subsetVolatility = (maxMin[0] - maxMin[1]) / maxMin[1];
            // 然后式用a + bx + c^2计算出不同时间段的权重
            double timeWeight = calTimeWeighted(i + 1, ac);
            totalWeight += timeWeight;
            totalVolatility += subsetVolatility * timeWeight;
        }
        // 计算volatility的加权平均值
        double volatility = totalVolatility / totalWeight;
        double adjustedVolatility = adjustVolatilityDuringMarketOpenPeriod(volatility, ac, datetime);
        return adjustedVolatility;
    }

    private static double[] calMaxMin(List<String> wapList) {
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

    private static double calTimeWeighted(int i, AppConfigManager.AppConfig.AlgorithmConfig ac) {
        return ac.getTimeWeightedA() + ac.getTimeWeightedB() * i + ac.getTimeWeightedC() * i * i;
    }

    public static double adjustVolatilityDuringMarketOpenPeriod(double volatility, AppConfigManager.AppConfig.AlgorithmConfig ac, String datetime) {
        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();
        String key = String.format("%s:%s:%s", accountId, symbol, secType);
        LocalTime marketOpenTime = LocalTime.of(9, 30, 0);

        LocalDateTime now = Utils.genUsDateTime(datetime, "yyyy-MM-dd HH:mm:ssXXX");
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

    private static double calOpenMarketVolatilityFactor(double volatility, AppConfigManager.AppConfig.AlgorithmConfig ac) {
        return ac.getVolatilityOpenMarketK() / (volatility * volatility) + 1;
    }

    private static double calCurrentVolatilityFactor(double openMarketVolatilityFactor, long passedSeconds, int totalSeconds, double k) {
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
}
