package capital.daphne.algorithms.close;

import capital.daphne.AppConfigManager;
import capital.daphne.algorithms.Sma;
import capital.daphne.models.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Table;

public class MACDZero implements CloseAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(Sma.class);
    private AppConfigManager.AppConfig.AlgorithmConfig ac;

    public MACDZero(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
    }

    public Signal getSignal(Table df, int position, int maxPosition) {
        Signal signal = new Signal();
        return signal;
    }
}
