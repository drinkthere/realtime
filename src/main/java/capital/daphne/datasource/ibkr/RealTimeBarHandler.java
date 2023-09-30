package capital.daphne.datasource.ibkr;

import capital.daphne.Db;
import com.ib.client.Decimal;
import com.ib.controller.Bar;
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
import java.util.stream.Collectors;

@Data
class BarInfo {
    private String date;
    private double vwap;
    private double volatility;
}

public class RealTimeBarHandler implements IbkrController.IRealTimeBarHandler {
    private static final Logger logger = LoggerFactory.getLogger(RealTimeBarHandler.class);

    private Map<String, List<BarInfo>> symbolToBarListMap;

    private Db db;

    private int minNumOfBars;

    private Map<String, double[]> symbolWapMap;

    private Map<String, Integer> barSblReqIdMap;

    public RealTimeBarHandler(Db db, List<String> symbols, int minNumOfBars) {
        this.db = db;
        this.minNumOfBars = minNumOfBars;
        symbolToBarListMap = new HashMap<>();
        for (String symbol : symbols) {
            symbolToBarListMap.put(symbol, new ArrayList<>());
        }
    }

    public void initWap(Map<String, double[]> symbolWapMap) {
        this.symbolWapMap = symbolWapMap;
    }

    public void setBarSblReqIdMap(Map<String, Integer> barSblReqIdMap) {
        this.barSblReqIdMap = barSblReqIdMap;
    }

    @Override
    synchronized public void realtimeBar(int reqId, Bar bar) {
        // 数据校验
        if (bar.time() <= 0 || bar.volume().compareTo(Decimal.ZERO) <= 0 || bar.volume().compareTo(Decimal.ZERO) <= 0) {
            logger.warn("bar data is invalid: " + bar.toString());
            return;
        }

        boolean dataUpdate = false;

        double wap = Double.parseDouble(bar.wap().toString());
        String symbol = barSblReqIdMap.entrySet().stream()
                .filter(entry -> entry.getValue() == reqId)
                .map(Map.Entry::getKey)
                .collect(Collectors.joining(", "));
        double[] wapArr = symbolWapMap.get(symbol);

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
        symbolWapMap.put(symbol, wapArr);

        if (dataUpdate) {
            // 更新当前交易日的max_wap和min_wap
            db.updateWapCache(symbol, currMaxWap, currMinWap);
        }

        BarInfo barInfo = new BarInfo();
        List<BarInfo> barList = symbolToBarListMap.get(symbol);
//        logger.info(symbolToBarListMap.toString());
//        logger.info(reqId + " " + symbol + " " + barList);
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
        int maxKeepNumOfBars = 2 * minNumOfBars;
        if (barList.size() > maxKeepNumOfBars) {
            int elementsToRemove = barList.size() - maxKeepNumOfBars;
            barList.subList(0, elementsToRemove).clear();
        }
        //logger.info(reqId + " | " + symbol + " " + barList);
        symbolToBarListMap.put(symbol, barList);
    }

    public Table getDataTable(String symbol) {
        List<BarInfo> barList = symbolToBarListMap.get(symbol);
        logger.debug("size=" + barList.size() + ", minNumOfBars=" + minNumOfBars);
        if (barList.size() < minNumOfBars) {
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
        String formattedTime = zonedDateTime.format(formatter);
        return formattedTime;
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
}
