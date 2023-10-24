package capital.daphne.services;

import capital.daphne.AppConfigManager;
import capital.daphne.DbManager;
import capital.daphne.algorithms.Algorithm;
import capital.daphne.algorithms.Ema;
import capital.daphne.algorithms.Sma;
import capital.daphne.algorithms.close.CloseAlgorithm;
import capital.daphne.algorithms.close.MACDSingal;
import capital.daphne.algorithms.close.MACDZero;
import capital.daphne.models.Signal;
import capital.daphne.utils.Utils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.Table;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SignalSvc {
    private static final Logger logger = LoggerFactory.getLogger(SignalSvc.class);

    private final BarSvc barService;

    private final PositionSvc positionService;

    private final Map<String, Algorithm> algoProcessorMap;

    private final Map<String, CloseAlgorithm> closeAlgoProcessorMap;

    public SignalSvc(List<AppConfigManager.AppConfig.AlgorithmConfig> algorithmConfigList) {
        barService = new BarSvc();
        positionService = new PositionSvc();

        algoProcessorMap = new HashMap<>();
        closeAlgoProcessorMap = new HashMap<>();
        for (AppConfigManager.AppConfig.AlgorithmConfig ac : algorithmConfigList) {
            String accountId = ac.getAccountId();
            String symbol = ac.getSymbol();
            String secType = ac.getSecType();

            Algorithm algoProcessor;
            switch (ac.getName()) {
                case "EMA":
                    algoProcessor = new Ema(ac);
                    break;
                case "SMA":
                default:
                    algoProcessor = new Sma(ac);
                    break;
            }
            algoProcessorMap.put(accountId + "." + symbol + "." + secType, algoProcessor);

            AppConfigManager.AppConfig.CloseAlgorithmConfig cac = ac.getCloseAlgo();
            if (cac != null) {
                CloseAlgorithm closeAlgoProcess = null;
                switch (cac.getMethod()) {
                    case "MACD_SIGNAL":
                        closeAlgoProcess = new MACDSingal(ac);
                        break;
                    case "MACD_ZERO":
                        closeAlgoProcess = new MACDZero(ac);
                        break;
                    default:
                        break;
                }
                closeAlgoProcessorMap.put(accountId + "." + symbol + "." + secType, closeAlgoProcess);
            }
        }
    }

    public Signal getTradeSignal(AppConfigManager.AppConfig.AlgorithmConfig ac) {
        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();
        String key = Utils.genKey(symbol, secType);

        // FUT全天交易，STK和CDF在正常交易区间交易
        if (!secType.equals("STK") || Utils.isMarketOpen()) {
            // 获取5s bar信息
            Table df = barService.getDataTable(key, ac.getNumStatsBars());
            if (df == null) {
                logger.info(key + " dataframe is not ready");
                return null;
            }

            // 获取position信息
            int position = positionService.getPosition(accountId, symbol, secType);
            int maxPosition = ac.getMaxPortfolioPositions();

            // 判断是否要下单
            Algorithm algoProcessor = algoProcessorMap.get(ac.getAccountId() + "." + key);
            Signal signal = algoProcessor.getSignal(df, position, maxPosition);

            if (signal != null && signal.isValid()) {
                return signal;
            }

            // 如果没有open的信号，但是有close的配置，尝试获取信号
            if (ac.getCloseAlgo() != null) {
                CloseAlgorithm closeAlgoProcessor = closeAlgoProcessorMap.get(ac.getAccountId() + "." + key);

                if (closeAlgoProcessor != null) {
                    return closeAlgoProcessor.getSignal(df, position, maxPosition);
                }
            }
            return null;
        } else {
            logger.info("market is not open");
            return null;
        }
    }

    public void saveSignal(Signal sig) {
        try (Connection connection = DbManager.getConnection()) {
            String insertSQL = "INSERT INTO tb_signal (account_id, uuid, symbol, sec_type, wap, quantity) VALUES (?, ?, ?, ?, ?, ?)";
            PreparedStatement insertStatement = connection.prepareStatement(insertSQL);
            insertStatement.setString(1, sig.getAccountId());
            insertStatement.setString(2, sig.getUuid());
            insertStatement.setString(3, sig.getSymbol());
            insertStatement.setString(4, sig.getSecType());
            insertStatement.setDouble(5, sig.getWap());
            insertStatement.setInt(6, sig.getQuantity());
            insertStatement.executeUpdate();
            logger.info("save signal successfully, signal: " + sig);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("save signal failed, error:" + e.getMessage());
        }
    }

    public void sendSignal(Signal signal) {
        try {
            AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
            String traderSrvUrl = String.format(
                    "http://%s:%d/%s",
                    appConfig.getHttp().getHost(),
                    appConfig.getHttp().getPort(),
                    appConfig.getHttp().getPath()
            );
            URL apiUrl = new URL(traderSrvUrl);
            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);


            JSONObject requestData = new JSONObject();
            requestData.put("accountId", signal.getAccountId());
            requestData.put("uuid", signal.getUuid());
            requestData.put("symbol", signal.getSymbol());
            requestData.put("secType", signal.getSecType());
            requestData.put("wap", signal.getWap());
            requestData.put("quantity", signal.getQuantity());
            requestData.put("orderType", signal.getOrderType());

            // 获取 JSON 字符串形式的请求体
            String requestBody = requestData.toString();

            // 将请求数据写入输出流
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = requestBody.getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            // 获取响应
            int responseCode = connection.getResponseCode();

            if (responseCode == HttpURLConnection.HTTP_OK) {
                // 读取响应数据
                try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String line;
                    StringBuilder response = new StringBuilder();
                    while ((line = in.readLine()) != null) {
                        response.append(line);
                    }
                    logger.info("Response: " + response.toString());
                }
            } else {
                logger.error("HTTP POST request failed with response code: " + responseCode);
            }
            connection.disconnect();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
