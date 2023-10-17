package capital.daphne.algorithms.close;

import capital.daphne.AppConfigManager;
import capital.daphne.algorithms.Sma;
import capital.daphne.models.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Table;

public class MACDSingal implements CloseAlgorithm {
    private static final Logger logger = LoggerFactory.getLogger(Sma.class);
    private AppConfigManager.AppConfig.AlgorithmConfig ac;


    public MACDSingal(AppConfigManager.AppConfig.AlgorithmConfig algorithmConfig) {
        ac = algorithmConfig;

    }

    public Signal getSignal(Table df, int position, int maxPosition) {
        Signal signal = new Signal();
        return signal;
    }
//
//    public Table generateBenchmarkColumn(Table df) {
//        AppConfigManager.AppConfig.CloseAlgorithmConfig closeAlgo = ac.getCloseAlgo();
//        int shortPeriod = closeAlgo.getMacdShortNumStatsBar();
//        double shortMultiplier = 2.0 / (shortPeriod + 1);
//
//        int longPeriod = closeAlgo.getMacdLongNumStatsBar();
//        double longMultiplier = 2.0 / (longPeriod + 1);
//
//        int signalPeriod = closeAlgo.getMacdSignalNumStatsBar();
//        double signalMultiplier = 2.0 / (signalPeriod + 1);
//
//        // Seed the EMA with the SMA value for its first data point
//        DoubleColumn prev_vwap = df.doubleColumn("prev_vwap");
//        DoubleColumn shortEma = Utils.ewm(prev_vwap, shortMultiplier, shortBenchmarkColumn, true, false, shortPeriod, shortPeriod - 1);
//        DoubleColumn longEma = Utils.ewm(prev_vwap, longMultiplier, longBenchmarkColumn, true, false, longPeriod, longPeriod - 1);
//
//        DoubleColumn macdLine = shortEma.subtract(longEma);
//        macdLine.setName(this.benchmarkColumn);
//        df.addColumns(macdLine);
//
//
//        DoubleColumn macdSignal = Utils.ewm(macdLine, signalMultiplier, signalBenchmarkColumn, true, false, signalPeriod, signalPeriod - 1);
//        df.addColumns(macdSignal);
//
//        return df;
//    }
}
