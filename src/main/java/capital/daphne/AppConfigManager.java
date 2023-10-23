package capital.daphne;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class AppConfigManager {
    private static AppConfigManager instance;
    private AppConfig appConfig;

    private AppConfigManager() {
        // 在构造函数中加载配置文件并创建 AppConfig 实例
        appConfig = loadConfig();
    }

    public static synchronized AppConfigManager getInstance() {
        if (instance == null) {
            instance = new AppConfigManager();
        }
        return instance;
    }

    public AppConfig getAppConfig() {
        return appConfig;
    }

    private AppConfig loadConfig() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.readValue(new File("config.json"), AppConfig.class);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Data
    public static class AppConfig {
        private Database database;
        private Redis redis;
        private Http http;
        private List<ContractConfig> contracts;
        private List<AlgorithmConfig> algorithms;

        @Data
        public static class Database {
            private String host;
            private int port;
            private String user;
            private String password;
            private String dbname;
            private int initialConnectionsSize;
            private int maxConnectionsTotal;
        }

        @Data
        public static class Redis {
            private String host;
            private int port;
            private String password;
        }

        @Data
        public static class Http {
            private String host;
            private int port;
            private String path;
        }

        @Data
        public static class ContractConfig {
            private String symbol;
            private String secType;
            private String lastTradeDateOrContractMonth;
            private String multiplier;
            private String exchange;
            private String primaryExchange;
            private String currency;
            private double minTicker;
        }

        @Data
        public static class AlgorithmConfig {
            private String name;
            private String accountId;
            private String symbol;
            private String secType;
            private boolean portfolioRequiredToClose;
            private int maxPortfolioPositions;
            private float positionSignalMarginOffset;
            private int orderSize;
            private int minIntervalBetweenSignal;
            private int numStatsBars;
            private float signalMargin;
            private int volatilityA;
            private int volatilityB;
            private int volatilityC;
            private float hardLimit;
            private String hardLimitClosePositionMethod;
            private int minDurationWhenReset;
            private CloseAlgorithmConfig closeAlgo;
        }

        @Data
        public static class CloseAlgorithmConfig {
            private String method;
            private int minDurationBeforeClose;
            private int maxDurationToClose;
            private int macdShortNumStatsBar;
            private int macdLongNumStatsBar;
            private int macdSignalNumStatsBar;
        }
    }

}