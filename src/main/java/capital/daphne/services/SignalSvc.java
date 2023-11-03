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

    // 判断是否在收盘前平仓的portfolioProcessMap
    private final Map<String, AlgorithmProcessor> closePortfolioProcessorMap;

    private final Map<String, AlgorithmProcessor> closeHardLimitProcessorMap;

    public SignalSvc(List<AppConfigManager.AppConfig.AlgorithmConfig> algorithmConfigList) {
        barService = new BarSvc();
        positionService = new PositionSvc();

        openAlgoProcessorMap = new HashMap<>();
        closeAlgoProcessorMap = new HashMap<>();
        closePortfolioProcessorMap = new HashMap<>();
        closeHardLimitProcessorMap = new HashMap<>();
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
                AlgorithmProcessor closeAlgoProcessor = loadAlgoProcessor("capital.daphne.algorithms.close", cac.getMethod(), ac);
                closeAlgoProcessorMap.put(algoKey, closeAlgoProcessor);
            }

            // 初始化closePortfolioProcessor
            AppConfigManager.AppConfig.ClosePortfolio cp = ac.getClosePortfolio();
            if (cp != null) {
                AlgorithmProcessor closeAlgoProcessor = loadAlgoProcessor("capital.daphne.algorithms.close", cp.getMethod(), ac);
                closePortfolioProcessorMap.put(algoKey, closeAlgoProcessor);
            }

            AppConfigManager.AppConfig.hardLimit hl = ac.getHardLimit();
            if (hl != null && hl.getMethod().equals("Reset")) {
                AlgorithmProcessor closeHardLimitProcessor = loadAlgoProcessor("capital.daphne.algorithms.close", hl.getMethod(), ac);
                closeHardLimitProcessorMap.put(algoKey, closeHardLimitProcessor);
            }
        }
    }

    public Signal getTradeSignal(AppConfigManager.AppConfig.AlgorithmConfig ac) {
        String accountId = ac.getAccountId();
        String symbol = ac.getSymbol();
        String secType = ac.getSecType();
        String dataKey = Utils.genKey(symbol, secType);
        String algoKey = ac.getAccountId() + ":" + dataKey;

        if (Utils.isTradingNow(symbol, secType, Utils.genUsDateTimeNow())) {
            // 获取bar信息
            Table df = barService.getDataTable(dataKey, ac.getNumStatsBars());
            if (df == null) {
                return null;
            }

            // 获取position信息
            int position = positionService.getPosition(accountId, symbol, secType);
            int maxPosition = ac.getMaxPortfolioPositions();

            // 判断是否配置了收盘前平仓的逻辑(e.g. 盘前10分钟平仓)
            AppConfigManager.AppConfig.ClosePortfolio closePortfolio = ac.getClosePortfolio();
            if (closePortfolio != null) {
                // 如果配置了，并且当前处于收盘前的平仓阶段, 无论有没有信号，都不会往下进行了
                if (Utils.isCloseToClosing(symbol, secType, Utils.genUsDateTimeNow(), closePortfolio.getSecondsBeforeMarketClose())) {
                    logger.info(String.format("symbol=%s, secType=%s, algoKey=%s is closing to close",
                            symbol, secType, algoKey));
                    AlgorithmProcessor closePortfolioProcessor = closePortfolioProcessorMap.get(algoKey);
                    if (closePortfolioProcessor == null) {
                        logger.error(String.format("symbol=%s, secType=%s, algoKey=%s can't not find closePortfolioProcessor",
                                symbol, secType, algoKey));
                        return null;
                    }
                    return closePortfolioProcessor.getSignal(df, position, maxPosition);
                }
            }

            // 判断是否要开仓, (open, e.g. SMA)
            AlgorithmProcessor openAlgoProcessor = openAlgoProcessorMap.get(algoKey);
            if (openAlgoProcessor != null) {
                Signal signal = openAlgoProcessor.getSignal(df, position, maxPosition);
                // 同一个标的的开仓和平仓信号不会在一个bar中处理，优先处理开仓信号，所以这里判断信号有效就先返回了
                if (signal != null && signal.isValid()) {
                    return signal;
                }
            }

            // 如果有平仓的配置，尝试获取平仓信号 (close, e.g. TrailingStop)
            AlgorithmProcessor closeAlgoProcessor = closeAlgoProcessorMap.get(algoKey);
            if (closeAlgoProcessor != null) {
                Signal signal = closeAlgoProcessor.getSignal(df, position, maxPosition);
                if (signal != null && signal.isValid()) {
                    return signal;
                }
            }


            // 如果有满仓减仓配置，尝试获取减仓信号(e.g. 当position达到上线，并且配置了reset参数）
            AlgorithmProcessor closeHardLimitProcessor = closeHardLimitProcessorMap.get(algoKey);
            if (closeHardLimitProcessor != null) {
                return closeHardLimitProcessor.getSignal(df, position, maxPosition);
            }

        } else {
            logger.info(String.format("symbol=%s, secType=%s, market is not open", symbol, secType));
            // cleaDirtyData(ac);
        }
        return null;
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

    private void cleaDirtyData(AppConfigManager.AppConfig.AlgorithmConfig ac) {
        // 清理垃圾数据， 如*LAST_ACTION， *ORDER_LIST等, *POSITION
        String benchmarkColumnName = ac.getName() + ac.getNumStatsBars();
        String redisKey = String.format("%s:%s:%s:%s:LAST_ACTION", ac.getAccountId(), ac.getSymbol(), ac.getSecType(), benchmarkColumnName);
        Utils.clearLastActionInfo(redisKey);
    }
}
