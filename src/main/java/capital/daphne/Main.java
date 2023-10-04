package capital.daphne;

import capital.daphne.datasource.Signal;
import capital.daphne.datasource.ibkr.Ibkr;
import capital.daphne.strategy.Strategy;
import capital.daphne.utils.Utils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // 加载配置文件
        logger.info("加载配置文件");
        String configFile;
        String projectRoot = System.getProperty("user.dir");
        if (args.length == 0) {
            configFile = projectRoot + "/config.json";
        } else {
            configFile = args[0];
        }

        ObjectMapper objectMapper = new ObjectMapper();
        AppConfig appConfig;
        try {
            appConfig = objectMapper.readValue(new File(configFile), AppConfig.class);
        } catch (IOException e) {
            logger.error("Can not load config file: " + e.getMessage());
            return;
        }

        logger.info("初始化数据对象：mysql");
        Db db = new Db(
                appConfig.getDatabase().getHost(),
                appConfig.getDatabase().getPort(),
                appConfig.getDatabase().getUser(),
                appConfig.getDatabase().getPassword(),
                appConfig.getDatabase().getDbname()
        );

        logger.info("初始化缓存对象：redis");
        JedisUtil.initializeJedisPool(
                appConfig.getRedis().getHost(),
                appConfig.getRedis().getPort(),
                appConfig.getRedis().getPassword()
        );

        logger.info("初始化数据源服务：ibkr");
        Ibkr ibkr = new Ibkr(appConfig, db);
        ibkr.connectTWS();

        logger.info("初始化wap信息");
        Map<String, double[]> symbolWapMap = db.loadWapCache(appConfig.getSymbols());
        ibkr.initWap(symbolWapMap);

        logger.info("启动IBKR监听服务, 监听ticker和5s bar");
        ibkr.startRealtimeWatcher();

        // 5s一次检查是否需要触发买/卖信号
        while (true) {
            try {
                long stime = System.currentTimeMillis();
                long ssecond = (stime / 1000) % 60;
                // 为了确保在收到回调消息后再处理，在1s、6s、11s.... 做处理
                if (ssecond % 5 == 1) {
                    // 判断是否是开盘时间
                    if (Utils.isMarketOpen()) {
                        List<Signal> signalList = ibkr.getTradeSignals();
                        if (signalList.size() > 0) {
                            for (Signal tradeSignal : signalList) {
                                logger.info(String.format(
                                        "http://%s:%d/%s",
                                        appConfig.getHttp().getHost(),
                                        appConfig.getHttp().getPort(),
                                        appConfig.getHttp().getPath()
                                ));
                                if (tradeSignal.isValid()) {
                                    UUID uuid = UUID.randomUUID();
                                    String uuidString = uuid.toString();
                                    tradeSignal.setUuid(uuidString);
                                    if (tradeSignal.getSide().equals(Strategy.TradeActionType.BUY)) {
                                        // buy时， price设置成ask price
                                        tradeSignal.setQuantity(tradeSignal.getQuantity());
                                        tradeSignal.setPrice(tradeSignal.getAskPrice());
                                    } else if (tradeSignal.getSide().equals(Strategy.TradeActionType.SELL)) {
                                        // sell时， price设置成bid price
                                        tradeSignal.setQuantity(0 - tradeSignal.getQuantity());
                                        tradeSignal.setPrice(tradeSignal.getBidPrice());
                                    }

                                    // 记录下单信息
                                    db.addSignal(tradeSignal);

                                    // 调用下单服务下单。
                                    sendSignal(
                                            String.format(
                                                    "http://%s:%d/%s",
                                                    appConfig.getHttp().getHost(),
                                                    appConfig.getHttp().getPort(),
                                                    appConfig.getHttp().getPath()
                                            ),
                                            tradeSignal
                                    );
                                }
                            }
                        }
                    } else {
                        logger.info("不在开盘时间");
                    }
                }

                long etime = System.currentTimeMillis();
                long sleepMillis = getSleepMillis(etime);
                TimeUnit.MILLISECONDS.sleep(sleepMillis);

            } catch (InterruptedException e) {
                //e.printStackTrace();
                logger.error("服务被中断, error:" + e.getMessage());
                return;
            }
        }
    }

    // 判断是否是美东时间，周一到周五的早上9:30到下午4:00之间（开盘期间）
    // todo 后续考虑把节假日也加上


    private static long getSleepMillis(long endTime) {
        long seconds = (endTime / 1000) % 60;
        long millis = endTime % 1000;
        long sleepMillis;
        if (seconds % 5 == 0) {
            sleepMillis = 1000 * 1 - millis;
        } else if (seconds % 5 == 1) {
            sleepMillis = 1000 * 5 - millis;
        } else {
            sleepMillis = 1000 * (6 - seconds % 5) - millis;
        }
        return sleepMillis;
    }

    private static void sendSignal(String url, Signal signal) {
        try {
            URL apiUrl = new URL(url);
            HttpURLConnection connection = (HttpURLConnection) apiUrl.openConnection();

            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoOutput(true);

            JSONObject requestData = new JSONObject();
            requestData.put("uuid", signal.getUuid());
            requestData.put("symbol", signal.getSymbol());
            requestData.put("price", signal.getPrice());
            requestData.put("wap", signal.getWap());
            requestData.put("quantity", signal.getQuantity());

            // 获取 JSON 字符串形式的请求体
            String requestBody = requestData.toString();


            // 将请求数据写入输出流
            try (OutputStream os = connection.getOutputStream()) {
                byte[] input = requestBody.getBytes("utf-8");
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