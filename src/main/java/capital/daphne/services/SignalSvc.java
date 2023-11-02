package capital.daphne.services;

import capital.daphne.AppConfigManager;
import capital.daphne.DbManager;
import capital.daphne.algorithms.AlgorithmProcessor;
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
import java.lang.reflect.Constructor;
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

    // 判断是否开仓的algoProcessorMap
    private final Map<String, AlgorithmProcessor> openAlgoProcessorMap;

    // 判断是否平仓的algoProcessorMap
    private final Map<String, AlgorithmProcessor> closeAlgoProcessorMap;

    public SignalSvc(List<AppConfigManager.AppConfig.AlgorithmConfig> algorithmConfigList) {
        barService = new BarSvc();
        positionService = new PositionSvc();

        openAlgoProcessorMap = new HashMap<>();
        closeAlgoProcessorMap = new HashMap<>();
        for (AppConfigManager.AppConfig.AlgorithmConfig ac : algorithmConfigList) {
            String accountId = ac.getAccountId();
            String symbol = ac.getSymbol();
            String secType = ac.getSecType();

            String algoKey = accountId + ":" + symbol + ":" + secType;
            // 初始化openAlgoProcessor
            AlgorithmProcessor openAlgoProcessor = loadAlgoProcessor("capital.daphne.algorithms", ac.getName(), ac);
            openAlgoProcessorMap.put(algoKey, openAlgoProcessor);

            // 初始化closeAlgoProcessor
            AppConfigManager.AppConfig.CloseAlgorithmConfig cac = ac.getCloseAlgo();
            if (cac != null) {
                AlgorithmProcessor closeAlgoProcess = loadAlgoProcessor("capital.daphne.algorithms.close", cac.getMethod(), ac);
                closeAlgoProcessorMap.put(algoKey, closeAlgoProcess);
            }
        }
    }

    public Signal getTradeSignal(AppConfigManager.AppConfig.AlgorithmConfig ac) {
        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();
        String dataKey = Utils.genKey(symbol, secType);
        String algoKey = ac.getAccountId() + ":" + dataKey;

        // todo 不同类型的标的，定义不同的MarketOpen时间
        if (!secType.equals("STK") || Utils.isMarketOpen()) {
            // 获取bar信息
            Table df = barService.getDataTable(dataKey, ac.getNumStatsBars());
            if (df == null) {
                return null;
            }

            // 获取position信息
            int position = positionService.getPosition(accountId, symbol, secType);
            int maxPosition = ac.getMaxPortfolioPositions();

            // 判断是否要开仓
            AlgorithmProcessor openAlgoProcessor = openAlgoProcessorMap.get(algoKey);
            if (openAlgoProcessor != null) {
                Signal signal = openAlgoProcessor.getSignal(df, position, maxPosition);
                // 同一个标的的开仓和平仓信号不会在一个bar中处理，优先处理开仓信号，所以这里判断信号有效就先返回了
                if (signal != null && signal.isValid()) {
                    return signal;
                }
            }

            // 如果有平仓的配置，尝试获取平仓信号
            if (ac.getCloseAlgo() != null) {
                AlgorithmProcessor closeAlgoProcessor = closeAlgoProcessorMap.get(algoKey);
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
            requestData.put("benchmarkColumn", signal.getBenchmarkColumn());

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

    private AlgorithmProcessor loadAlgoProcessor(String packageName, String className, AppConfigManager.AppConfig.AlgorithmConfig ac) {
        AlgorithmProcessor algoProcessor = null;
        try {
            String pacakgeToClassName = packageName + "." + className;
            // 使用反射加载类
            Class<?> clazz = Class.forName(pacakgeToClassName);
            Constructor<?> constructor = clazz.getConstructor(AppConfigManager.AppConfig.AlgorithmConfig.class);
            algoProcessor = (AlgorithmProcessor) constructor.newInstance(ac);

            logger.info("Successfully created an instance of: " + className);
            logger.info("Instance: " + algoProcessor);
            return algoProcessor;
        } catch (ClassNotFoundException e) {
            logger.error("Class not found: " + className);
        } catch (Exception e) {
            logger.error("Error creating instance for: " + className);
            e.printStackTrace();
        }
        return algoProcessor;
    }
}
