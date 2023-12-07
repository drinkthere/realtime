package order;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.models.Signal;
import capital.daphne.models.WapCache;
import capital.daphne.services.SignalSvc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ib.client.Types;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class OrderTest {

    @Test
    public void placeOrder() {
        JedisManager.initializeJedisPool();
        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        SignalSvc signalSvc = new SignalSvc(appConfig.getAlgorithms());
        Signal signal = new Signal();
        signal.setValid(true);
        signal.setAccountId("DU6380369");
        signal.setSymbol("EUR");
        signal.setSecType("CASH");
        signal.setWap(1.08785);
        signal.setQuantity(25000);
        signal.setOrderType(Signal.OrderType.OPEN);
        signal.setBenchmarkColumn("sma18");

        signalSvc.sendSignal(signal);
    }

    @Test
    public void placeOptionOrder() {
        JedisManager.initializeJedisPool();
        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        SignalSvc signalSvc = new SignalSvc(appConfig.getAlgorithms());

        String action = "SELL";
        String right = "Call";
        Signal optionSignal = new Signal();
        optionSignal.setAccountId("DU6380369");
        optionSignal.setUuid(UUID.randomUUID().toString());
        optionSignal.setSymbol("SPX");
        optionSignal.setSecType(Types.SecType.OPT.name());
        optionSignal.setWap(1);
        int quantity = action.equals("BUY") ? 1 : -1;
        optionSignal.setQuantity(quantity);
        optionSignal.setOrderType(Signal.OrderType.OPEN);
        optionSignal.setOptionRight(right);
        optionSignal.setBenchmarkColumn("EMA18");

        signalSvc.sendSignal(optionSignal);
    }

    @Test
    public void compareDateTime() {
        String date_us = "2023-10-25 08:37:00-04:00";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
        LocalDateTime datetime = zonedDateTime.toLocalDateTime();
        LocalTime time = datetime.toLocalTime();


        ZonedDateTime usEasternTime = ZonedDateTime.now()
                .withZoneSameInstant(java.time.ZoneId.of("US/Eastern"));
        DateTimeFormatter usFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");

        // 格式化时间
        String formattedTime = usEasternTime.format(usFormatter);


        ZonedDateTime parse = ZonedDateTime.parse(formattedTime, formatter);
        System.out.println(datetime + "|" + formattedTime + "|" + parse.toLocalDateTime());
    }

    @Test
    public void splitString() {
        String actionInfo = "aaa:bbb:ccc:ddd";
        String[] split = actionInfo.split(":");
        String action = split[0];
        String datetime = split[1];
        System.out.println(action + "-" + datetime);
    }

    @Test
    public void calLimitPrice() {
        double bidPrice = 1.251911141356789;
        float slipage = 0.001f;
        double minTicker = 0.00005;
        int decimals = 5;

        double lmtPrice = formatLmtPrice(bidPrice / (1 + slipage), decimals, minTicker);

        System.out.println(lmtPrice);
    }

    private double formatLmtPrice(double bidPrice, int decimals, double minTick) {
        BigDecimal bdBidPrice = BigDecimal.valueOf(bidPrice);
        BigDecimal minTickBigDecimal = BigDecimal.valueOf(minTick);

        // 1. 保留小数位不超过 decimals
        bdBidPrice = bdBidPrice.setScale(decimals, RoundingMode.HALF_UP);

        // 2. 计算最接近的满足 minTick 的价格
        bdBidPrice = bdBidPrice.divide(minTickBigDecimal, 0, RoundingMode.HALF_UP).multiply(minTickBigDecimal);

        return bdBidPrice.doubleValue();
    }

    @Test
    public void parseWapMaxMin() {
        try {
            String storedWapMaxMinJson = "{\"maxPriceSinceLastOrder\":414.7813292065279,\"minPriceSinceLastOrder\":414.5507109157195}";
            ObjectMapper objectMapper = new ObjectMapper();
            WapCache wapMaxMin = objectMapper.readValue(storedWapMaxMinJson, new TypeReference<>() {
            });
            boolean b = wapMaxMin.getMaxWap() == Double.MIN_VALUE || wapMaxMin.getMinWap() == Double.MAX_VALUE;
            System.out.println(String.format("xx %b", b));
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void formatUsDate() {
        try {
            String timeStr = "20231103 09:21:55 US/Eastern";
            ZoneId easternTimeZone = ZoneId.of("America/New_York");

            ZonedDateTime zonedDateTime = ZonedDateTime.parse(timeStr, java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV").withZone(easternTimeZone));

            System.out.println(zonedDateTime.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX")));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void testVolatility() {
        double openMarketVolatilityFactor = 1.5;
        int total = 600;
        for (int i = 1; i <= total; i++) {
            double f = calCurrentVolatilityFactor(openMarketVolatilityFactor, i, total, 0.618);
            System.out.println(f);
        }
    }

    @Test
    public void testOther() {
        int quantity = -100;
        int position = -100;
        double maxWap = 189.210000;
        double minWap = 189.126914;
        double wap = 189.14;
        double threshold = 0.0003;

        if ((quantity > 0 && position > 0
                && maxWap > 0 && wap <= (1 - threshold) * maxWap) ||
                (quantity < 0 && position < 0)
                        && minWap > 0 && wap >= (1 + threshold) * minWap) {
            System.out.println("xxx");
        } else {
            System.out.println("yyy");
        }
    }

    private double calCurrentVolatilityFactor(double openMarketVolatilityFactor, int passedSeconds, int totalSeconds, double k) {
        //return Math.pow((float) passedSeconds / totalSeconds, k);

        //openMarketVolatilityFactor * (1 - Math.pow(1 - 1 / openMarketVolatilityFactor, Math.pow(passedSeconds /totalSeconds, -k)));
        return openMarketVolatilityFactor * (1 - Math.pow(1 - 1 / openMarketVolatilityFactor, Math.pow((float) passedSeconds / totalSeconds, -k)));
    }

    @Test
    public void testDte() throws ParseException {
        List<String> expirations = List.of("20240119", "20231220", "20240419", "20240531", "20240112", "20240430", "20240216", "20240315", "20240930", "20250117", "20231219", "20231218", "20231212", "20231211", "20231214", "20231213", "20240229", "20240328", "20240628", "20240920", "20240105", "20240621", "20260116", "20251219", "20240131", "20241220", "20231208", "20231207", "20231229", "20250321", "20231206", "20250620", "20240531", "20231222");
        int thresholdDays = 2;

        LocalDate resultDate = calculateResultDate(expirations, thresholdDays);
        System.out.println(resultDate);
    }

    /**
     * 1. 对expirations进行排序，并且过滤expirations，得到 日期 - 当前日期 >=2 的交易日列表，存储到validTradingDates中
     * 2. 将validTradingDates的第一项作为startDate， 得到最近的周五的日期nearestFriday，如果nearestFriday在list中，就直接返回nearestFriday
     * 3. 如果nearestFriday不在list中，获取validTradingDates中 < nearestFriday的上一个日期closestDateToNearestFriday, 返回closestDateToNearestFriday
     */
    private LocalDate calculateResultDate(List<String> expirations, int thresholdDays) throws ParseException {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        LocalDate today = LocalDate.now();

        List<LocalDate> validTradingDates = new ArrayList<>();
        for (String expiration : expirations) {
            Date expirationDate = dateFormat.parse(expiration);
            LocalDate localDate = expirationDate.toInstant().atZone(java.time.ZoneId.systemDefault()).toLocalDate();
            if (localDate.isAfter(today.plusDays(thresholdDays - 1))) {
                validTradingDates.add(localDate);
            }
        }
        Collections.sort(validTradingDates);

        LocalDate startDate = validTradingDates.get(0);
        LocalDate nearestFriday = getNearestFriday(startDate);
        if (validTradingDates.contains(nearestFriday)) {
            return nearestFriday;
        }
        LocalDate closestDateToNearestFriday = findClosestDate(nearestFriday, validTradingDates);
        return closestDateToNearestFriday;
    }

    public LocalDate getNearestFriday(LocalDate date) {
        // Calculate the current day of the week
        DayOfWeek dayOfWeek = date.getDayOfWeek();

        // Calculate the days needed to reach the next Friday
        int daysToAdd = DayOfWeek.FRIDAY.getValue() - dayOfWeek.getValue();
        if (daysToAdd < 0) {
            // If the current day is after Friday, add days to reach the next Friday
            daysToAdd += 7;
        }

        // Add the calculated days to get the nearest Friday
        return date.plusDays(daysToAdd);
    }

    public LocalDate findClosestDate(LocalDate targetDate, List<LocalDate> sortedDateList) {
        LocalDate closestDate = null;

        for (LocalDate date : sortedDateList) {
            if (date.isBefore(targetDate)) {
                closestDate = date;
            } else {
                break; // Stop iterating when we reach a date after targetDate
            }
        }
        return closestDate;
    }
}

