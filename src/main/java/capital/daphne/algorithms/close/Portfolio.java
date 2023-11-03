package capital.daphne.algorithms.close;

import capital.daphne.AppConfigManager;
import capital.daphne.algorithms.AlgorithmProcessor;
import capital.daphne.models.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.util.UUID;

public class Portfolio implements AlgorithmProcessor {

    private static final Logger logger = LoggerFactory.getLogger(Portfolio.class);
    private final AppConfigManager.AppConfig.AlgorithmConfig ac;

    public Portfolio(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
    }

    @Override
    public Signal getSignal(Table df, int position, int maxPosition) {
        if (position == 0) {
            return null;
        }

        // 有仓位，发送平仓信号
        Row row = df.row(df.rowCount() - 1);

        Signal signal = new Signal();
        signal.setValid(true);
        signal.setAccountId(ac.getAccountId());
        signal.setUuid(UUID.randomUUID().toString());
        signal.setSymbol(ac.getSymbol());
        signal.setSecType(ac.getSecType());
        signal.setWap(row.getDouble("vwap"));
        signal.setQuantity(-position);
        signal.setOrderType(Signal.OrderType.CLOSE);
        signal.setBenchmarkColumn("closePortfolio");
        return signal;
    }
}
