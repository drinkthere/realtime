package order;

import capital.daphne.AppConfigManager;
import capital.daphne.models.Signal;
import capital.daphne.services.SignalSvc;
import org.testng.annotations.Test;

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
        signal.setSymbol("AUD");
        signal.setSecType("CASH");
        signal.setWap(423.15);
        signal.setQuantity(-50000);
        signal.setOrderType(Signal.OrderType.CLOSE);
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
        String actionInfo = "SELL|2023-10-25T08:20:02.885152";
        String[] split = actionInfo.split("\\|");
        String action = split[0];
        String datetime = split[1];
        System.out.println(action + "-" + datetime);
    }
}
