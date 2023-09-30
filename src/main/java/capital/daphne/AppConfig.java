package capital.daphne;

import lombok.Data;

import java.util.List;

@Data
public class AppConfig {
    private String apiKey;
    private Http http;
    private Database database;
    private Tws tws;
    private List<SymbolItem> symbols;
    private int numStatsBars;
    private String strategy;
    private boolean portfolioRequiredToClose;
    private int maxPortfolioPositions;
    private float positionSignalMarginOffset;
    private int minDurationAfterSignal;
    private int minIntervalBetweenSignal;
    private float signalMargin;
    private float volatilityA;
    private float volatilityB;
    private float volatilityC;
    private boolean hardLimit;
    private int orderSize;


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
    public static class Tws {
        private String host;
        private int port;
        private int clientId;
    }

    @Data
    public static class SymbolItem {
        private String symbol;
        private String exchange;
        private String primaryExchange;
        private String currency;
        private String strategy;
    }
}
