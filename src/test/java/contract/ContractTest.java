package contract;

import capital.daphne.JedisManager;
import capital.daphne.utils.Utils;
import org.testng.annotations.Test;

import java.time.LocalDateTime;

public class ContractTest {
    public ContractTest() {
        JedisManager.initializeJedisPool();
    }

    @Test
    public void isMarketOpenTest() {
        System.out.println("当前美东时间的 LocalDateTime: " + Utils.genUsDateTimeNow());
        String symbol = "SPY";
        String secType = "STK";
        // 20231101:0400-20231101:2000;20231102:0400-20231102:2000;20231103:0400-20231103:2000;20231104:CLOSED;20231105:CLOSED;20231106:0400-20231106:2000
        // STK 未到盘前
        LocalDateTime dateTime = Utils.genUsDateTime("2023-11-01 03:59", "yyyy-MM-dd HH:mm");
        boolean marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // STK 盘前开始，但是不交易
        dateTime = Utils.genUsDateTime("2023-11-01 05:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // STK 盘中
        dateTime = Utils.genUsDateTime("2023-11-06 09:33", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(marketOpen);

        // STK 盘后, 但是不交易
        dateTime = Utils.genUsDateTime("2023-11-01 16:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // STK STK 已过盘后
        dateTime = Utils.genUsDateTime("2023-11-01 20:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // 休市
        dateTime = Utils.genUsDateTime("2023-11-04 09:50", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);


        symbol = "QQQ";
        secType = "CFD";
        // 20231101:0930-20231101:1600;20231102:0930-20231102:1600;20231103:0930-20231103:1600;20231104:CLOSED;20231105:CLOSED;20231106:0930-20231106:1600
        // STK 未开盘
        dateTime = Utils.genUsDateTime("2023-11-01 09:29", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // STK 开盘
        dateTime = Utils.genUsDateTime("2023-11-01 09:30", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(marketOpen);

        // STK 盘中
        dateTime = Utils.genUsDateTime("2023-11-01 10:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(marketOpen);

        // STK 收盘
        dateTime = Utils.genUsDateTime("2023-11-01 16:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // 休市
        dateTime = Utils.genUsDateTime("2023-11-05 09:50", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);


        symbol = "ES";
        secType = "FUT";
        // 20231031:1700-20231101:1600;20231101:1700-20231102:1600;20231102:1700-20231103:1600;20231104:CLOSED;20231105:1700-20231106:1600;20231106:1700-20231107:1600
        // FUT 未开盘
        dateTime = Utils.genUsDateTime("2023-11-01 16:29", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // FUT 开盘
        dateTime = Utils.genUsDateTime("2023-11-01 17:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(marketOpen);

        // FUT 盘中
        dateTime = Utils.genUsDateTime("2023-11-01 18:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(marketOpen);

        // FUT 收盘
        dateTime = Utils.genUsDateTime("2023-11-02 16:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // FUT 休市
        dateTime = Utils.genUsDateTime("2023-11-03 18:50", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        symbol = "AUD";
        secType = "CASH";
        // 20231031:1715-20231101:1700;20231101:1715-20231102:1700;20231102:1715-20231103:1700;20231104:CLOSED;20231105:1715-20231106:1700;20231106:1715-20231107:1700
        // CASH 未开盘
        dateTime = Utils.genUsDateTime("2023-11-22 17:16", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // CASH 开盘
        dateTime = Utils.genUsDateTime("2023-11-01 17:15", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(marketOpen);

        // CASH 盘中
        dateTime = Utils.genUsDateTime("2023-11-01 18:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(marketOpen);

        // CASH 收盘
        dateTime = Utils.genUsDateTime("2023-11-02 17:00", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);

        // CASH 休市
        dateTime = Utils.genUsDateTime("2023-11-03 18:50", "yyyy-MM-dd HH:mm");
        marketOpen = Utils.isTradingNow(symbol, secType, dateTime, 0);
        System.out.println(!marketOpen);
    }

    @Test
    public void isMarketClosingToCloseTest() {
        System.out.println("当前美东时间的 LocalDateTime: " + Utils.genUsDateTimeNow());
        String symbol = "SPY";
        String secType = "STK";
        int seconds = 600;
        // 20231101:0400-20231101:2000;20231102:0400-20231102:2000;20231103:0400-20231103:2000;20231104:CLOSED;20231105:CLOSED;20231106:0400-20231106:2000
        // STK 未到收盘前600s
        LocalDateTime dateTime = Utils.genUsDateTime("2023-11-01 13:30", "yyyy-MM-dd HH:mm");
        boolean isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // STK 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-01 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // STK 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-02 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // STK 过了收盘时间
        dateTime = Utils.genUsDateTime("2023-11-01 16:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // STK 在休市期间
        dateTime = Utils.genUsDateTime("2023-11-04 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        symbol = "QQQ";
        secType = "CFD";
        // 20231101:0930-20231101:1600;20231102:0930-20231102:1600;20231103:0930-20231103:1600;20231104:CLOSED;20231105:CLOSED;20231106:0930-20231106:1600
        // CFD 不在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-01 08:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // CFD 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-01 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // CFD 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-02 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // CFD 过了收盘时间
        dateTime = Utils.genUsDateTime("2023-11-01 16:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // CFD 在休市期间
        dateTime = Utils.genUsDateTime("2023-11-04 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        symbol = "ES";
        secType = "FUT";
        // 20231031:1700-20231101:1600;20231101:1700-20231102:1600;20231102:1700-20231103:1600;20231104:CLOSED;20231105:1700-20231106:1600;20231106:1700-20231107:1600
        // FUT 不在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-01 16:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // FUT 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-01 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // FUT 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-02 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // FUT 过了收盘时间
        dateTime = Utils.genUsDateTime("2023-11-01 16:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // FUT 在休市期间
        dateTime = Utils.genUsDateTime("2023-11-04 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        symbol = "EUR";
        secType = "CASH";
        // 20231031:1715-20231101:1700;20231101:1715-20231102:1700;20231102:1715-20231103:1700;20231104:CLOSED;20231105:1715-20231106:1700;20231106:1715-20231107:1700
        // CASH 不在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-01 17:08", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // CASH 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-01 16:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // CASH 在收盘前600s内
        dateTime = Utils.genUsDateTime("2023-11-02 16:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(isClosingToClose);

        // CASH 过了收盘时间
        dateTime = Utils.genUsDateTime("2023-11-01 17:06", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

        // CASH 在休市期间
        dateTime = Utils.genUsDateTime("2023-11-04 15:51", "yyyy-MM-dd HH:mm");
        isClosingToClose = Utils.isCloseToClosing(symbol, secType, dateTime, seconds);
        System.out.println(!isClosingToClose);

    }
}
