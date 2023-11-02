package order;

import capital.daphne.AppConfigManager;
import capital.daphne.models.Signal;
import capital.daphne.models.WapMaxMin;
import capital.daphne.services.SignalSvc;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class OrderTest {

    @Test
    public void placeOrder() {
        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        SignalSvc signalSvc = new SignalSvc(appConfig.getAlgorithms());
        Signal signal = new Signal();
        signal.setValid(true);
        signal.setAccountId("DU6380369");
        signal.setUuid(UUID.randomUUID().toString());
        signal.setSymbol("EUR");
        signal.setSecType("CASH");
        signal.setWap(1.05628);
        signal.setQuantity(20000);
        signal.setOrderType(Signal.OrderType.OPEN);
        signal.setBenchmarkColumn("sma18");

        signalSvc.sendSignal(signal);
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
            WapMaxMin wapMaxMin = objectMapper.readValue(storedWapMaxMinJson, new TypeReference<>() {
            });
            boolean b = wapMaxMin.getMaxPriceSinceLastOrder() == Double.MIN_VALUE || wapMaxMin.getMinPriceSinceLastOrder() == Double.MAX_VALUE;
            System.out.println(String.format("xx %b", b));
        } catch (JsonMappingException e) {
            throw new RuntimeException(e);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

    }
}
