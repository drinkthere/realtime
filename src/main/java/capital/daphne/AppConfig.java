package capital.daphne;

import lombok.Data;

import java.util.List;

@Data
public class AppConfig {
    private Database database;
    private Redis redis;
    private Tws tws;
    private List<SymbolConfig> symbols;

    @Data
    public static class Strategy {
        private String name;
        private int maxPortfolioPositions;
        private float positionSignalMarginOffset;
        private float signalMargin;
        private float volatilityA;
        private float volatilityB;
        private float volatilityC;
        private int orderSize;
        private int numStatsBars;
        private boolean portfolioRequiredToClose;
        private int minDurationAfterSignal;
        private int minIntervalBetweenSignal;
        private boolean hardLimit;
    }

    @Data
    public static class Http {
        private String host;
        private int port;
        private String path;
    }

    @Data
    public static class Database {
        private String host;
        private int port;
        private String user;
        private String password;
        private String dbname;
    }

    @Data
    public static class Redis {
        private String host;
        private int port;
        private String password;
    }

    @Data
    public static class Tws {
        private String host;
        private int port;
        private int clientId;
    }

    @Data
    public static class Rewrite {
        private String symbol;
        private String secType;
        private int orderSize;
        private int maxPortfolioPositions;
    }

    @Data
    public static class Parallel {
        private String symbol;
        private String secType;
        private int orderSize;
    }

    @Data
    public static class SymbolConfig {
        private String symbol;
        private String secType;
        private String lastTradeDateOrContractMonth;
        private String multiplier;
        private String exchange;
        private String primaryExchange;
        private String currency;
        private Strategy strategy;
        // 如果存在就替换成其他contract
        private Rewrite rewrite;
        // 如果存在就同时发送其他contract
        private Parallel parallel;
        private Http http;
    }
}
