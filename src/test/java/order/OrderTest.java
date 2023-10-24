package order;

import capital.daphne.AppConfigManager;
import capital.daphne.models.Signal;
import capital.daphne.services.SignalSvc;
import org.testng.annotations.Test;

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
        signal.setWap(1.06442);
        signal.setQuantity(1000);
        signal.setOrderType(Signal.OrderType.OPEN);

        signalSvc.sendSignal(signal);
    }
}
