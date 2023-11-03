package capital.daphne.algorithms.close;

import capital.daphne.AppConfigManager;
import capital.daphne.algorithms.AlgorithmProcessor;
import capital.daphne.models.Signal;
import capital.daphne.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.Table;

import java.time.LocalDateTime;
import java.util.UUID;

public class Reset implements AlgorithmProcessor {

    private static final Logger logger = LoggerFactory.getLogger(Reset.class);
    private final AppConfigManager.AppConfig.AlgorithmConfig ac;

    private LocalDateTime resetDatetime;

    public Reset(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;
        resetDatetime = null;
    }

    @Override
    public Signal getSignal(Table df, int position, int maxPosition) {
        if (position == 0) {
            return null;
        }
        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();

        AppConfigManager.AppConfig.hardLimit hl = ac.getHardLimit();
        Row row = df.row(df.rowCount() - 1);

        LocalDateTime datetime = Utils.genUsDateTime(row.getString("date_us"), "yyyy-MM-dd HH:mm:ssXXX");

        // 有仓位，检查是否可以平仓
        if (Math.abs(position) >= maxPosition) {
            if (resetDatetime == null) {
                resetDatetime = datetime.plusSeconds(hl.getMinDurationWhenReset());
                // 如果延时之后，reset时间已经超出交易时间，就马上触发
                if (!Utils.isTradingNow(symbol, secType, resetDatetime)) {
                    resetDatetime = datetime;
                }
            }
            if (datetime.isAfter(resetDatetime) || datetime.equals(resetDatetime)) {
                resetDatetime = null;

                Signal signal = new Signal();
                signal.setValid(true);
                signal.setAccountId(accountId);
                signal.setUuid(UUID.randomUUID().toString());
                signal.setSymbol(symbol);
                signal.setSecType(secType);
                signal.setWap(row.getDouble("vwap"));
                signal.setQuantity(-position);
                signal.setOrderType(Signal.OrderType.CLOSE);
                signal.setBenchmarkColumn("hardLimitReset");
                return signal;

            }
        } else {
            resetDatetime = null;
        }

        return null;
    }
}
