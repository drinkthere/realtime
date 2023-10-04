package capital.daphne;

import lombok.Data;

import java.util.List;

@Data
public class AppConfig {
    private Http http;
    private Database database;
    private Redis redis;
    private Tws tws;
    private List<SymbolConfig> symbols;


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
    public static class SymbolConfig {
        private String symbol;
        private String exchange;
        private String primaryExchange;
        private String currency;
        private String strategy;
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
}
