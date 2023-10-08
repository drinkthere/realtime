package capital.daphne.datasource.ibkr;

import capital.daphne.AppConfig;
import capital.daphne.Db;
import capital.daphne.utils.Utils;
import com.ib.client.Decimal;
import com.ib.controller.Bar;
import com.mysql.cj.util.StringUtils;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
class BarInfo {
    private String date;
    private double vwap;
    private double volatility;
}

public class RealTimeBarHandler implements IbkrController.IRealTimeBarHandler {
    private static final Logger logger = LoggerFactory.getLogger(RealTimeBarHandler.class);
    private static final int maxWrongWapNum = 2;
    private final Map<Integer, String> reqIdKeyMap;
    private final Map<String, List<BarInfo>> keyBarListMap;
    private final Db db;
    private final Map<String, Integer> keyMinBarNumMap;
    private Map<String, double[]> keyWapMap;
    private int wrongWapNum;

    public RealTimeBarHandler(Db db, List<AppConfig.SymbolConfig> symbolsConfig) {
        this.db = db;
        keyBarListMap = new HashMap<>();
        keyMinBarNumMap = new HashMap<>();
        for (AppConfig.SymbolConfig sc : symbolsConfig) {
            String key = Utils.genKey(sc.getSymbol(), sc.getSecType());
            keyBarListMap.put(key, new ArrayList<>());
            keyMinBarNumMap.put(key, sc.getStrategy().getNumStatsBars() + 1);
        }
        reqIdKeyMap = new HashMap<>();
        wrongWapNum = 0;
    }

    public void initWap(Map<String, double[]> sblWapMap) {
        this.keyWapMap = sblWapMap;
    }

    public void bindReqIdKey(String key, int reqId) {
        reqIdKeyMap.put(reqId, key);
    }

    @Override
    synchronized public void realtimeBar(int reqId, Bar bar) {
        logger.debug("o:" + bar.open() + "|h:" + bar.high() + "|l:" + bar.low() + "|c:" + bar.close() + "|vw:" + bar.wap() + "|v:" + bar.volume());
        if (bar.time() <= 0) {
            logger.warn(String.format("bar data is invalid: time=%s", bar.time()));
            return;
        }
        String key = reqIdKeyMap.get(reqId);

        double vwap = Double.parseDouble(bar.wap().toString());
        double wap;
        if (bar.volume().compareTo(Decimal.ZERO) <= 0) {
            wap = (bar.open() + bar.high() + bar.low() + bar.close()) / 4;
            if (wap / vwap >= 1.001 || wap / vwap <= 0.999) {
                logger.warn(String.format("bar data is invalid: volume=0, vwap=%s, wap=%s, exceed:0.1%%", vwap, wap));
                wrongWapNum++;
                // 如果超过最大连续出错的数量，就清空所有的bar数据，重新计算
                if (wrongWapNum >= maxWrongWapNum) {
                    logger.warn("vwap exceed 0.1% beyond " + maxWrongWapNum + " times");
                    clearBarList(key);
                }
                return;
            }
        } else {
            wap = vwap;
        }

        // 走到这里，说明wap没问题，重置wrongWapNum
        wrongWapNum = 0;
        logger.debug("o:" + bar.open() + "|h:" + bar.high() + "|l:" + bar.low() + "|c:" + bar.close() + "|vw:" + vwap + "|cw:" + wap + "|v:" + bar.volume());

        boolean dataUpdate = false;
        double[] wapArr = keyWapMap.get(key);

        //wapArr[0 -> prevMaxWap, 1 -> prevMinWap, 2 -> currMaxWap, 3 -> currMinWap]
        double currMaxWap = wapArr[2];
        double currMinWap = wapArr[3];
        if (wap > currMaxWap) {
            currMaxWap = wap;
            dataUpdate = true;
        }
        if (currMinWap == 0 || wap < currMinWap) {
            currMinWap = wap;
            dataUpdate = true;
        }
        wapArr[2] = currMaxWap;
        wapArr[3] = currMinWap;
        keyWapMap.put(key, wapArr);

        if (dataUpdate) {
            // 更新当前交易日的max_wap和min_wap
            List<String> splitArr = StringUtils.split(key, ".", true);
            String symbol = splitArr.get(0);
            String secType = splitArr.get(1);
            db.updateWapCache(symbol, secType, currMaxWap, currMinWap);
        }

        BarInfo barInfo = new BarInfo();
        List<BarInfo> barList = keyBarListMap.get(key);
        try {
            String date = parseTime(bar.time());
            barInfo.setDate(date);
            barInfo.setVwap(wap);
            double volatility = calVolatility(wap, wapArr);
            barInfo.setVolatility(volatility);
            barList.add(barInfo);
        } catch (Exception e) {
            logger.warn(String.format("parse timeStr=%s failed, err:%s", bar.timeStr(), e.getMessage()));
            return;
        }

        // 只保留2倍minNumOfBar的bar数据
        int minBarNum = keyMinBarNumMap.get(key);
        int maxKeepNumOfBars = 2 * minBarNum;
        if (barList.size() > maxKeepNumOfBars) {
            int elementsToRemove = barList.size() - maxKeepNumOfBars;
            barList.subList(0, elementsToRemove).clear();
        }
        //logger.info(reqId + " | " + symbol + " " + barList);
        keyBarListMap.put(key, barList);
    }

    synchronized public Table getDataTable(String key) {
        int minBarNum = keyMinBarNumMap.get(key);
        List<BarInfo> barList = keyBarListMap.get(key);
        logger.debug("size=" + barList.size() + ", minNumOfBars=" + minBarNum);
        if (barList.size() < minBarNum) {
            return null;
        }

        Table dataframe = Table.create("IBKR Bar Dataframe");
        // 添加列到表格
        dataframe.addColumns(
                StringColumn.create("date_us"),
                DoubleColumn.create("vwap"),
                DoubleColumn.create("volatility"));
        for (BarInfo bar : barList) {
            dataframe.stringColumn("date_us").append(bar.getDate());
            dataframe.doubleColumn("vwap").append(bar.getVwap());
            dataframe.doubleColumn("volatility").append(bar.getVolatility());
        }
        DoubleColumn prevVWapColumn = dataframe.doubleColumn("vwap").lag(1);
        dataframe.addColumns(prevVWapColumn.setName("prev_vwap"));
        return dataframe;
    }

    private String parseTime(long timestamp) throws ParseException {
        // 使用Instant将时间戳转换为ZonedDateTime，并将其时区设置为美东时区
        Instant instant = Instant.ofEpochSecond(timestamp);
        ZonedDateTime zonedDateTime = instant.atZone(ZoneId.of("US/Eastern"));

        // 使用自定义的DateTimeFormatter格式化为指定格式
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        return zonedDateTime.format(formatter);
    }

    private double calVolatility(double wap, double[] wapArr) {
        double prevMaxWap = wapArr[0];
        double prevMinWap = wapArr[1];
        double currMaxWap = wapArr[2];
        double currMinWap = wapArr[3];


        double maxWap = Math.max(prevMaxWap, currMaxWap);
        double minWap = Math.min(prevMinWap, currMinWap);
        minWap = minWap == 0 ? wap : minWap;
        return (maxWap - minWap) / minWap;
    }

    synchronized private void clearBarList(String key) {
        List<BarInfo> barList = keyBarListMap.get(key);
        barList.clear();
        keyBarListMap.put(key, barList);
    }
}
