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
        signal.setSymbol("AUD");
        signal.setSecType("CASH");
        signal.setWap(423.15);
        signal.setQuantity(-315000);
        signal.setOrderType(Signal.OrderType.OPEN);

        signalSvc.sendSignal(signal);
    }
}
