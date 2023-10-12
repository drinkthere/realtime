package capital.daphne;

import capital.daphne.datasource.Signal;
import capital.daphne.datasource.ibkr.Ibkr;
import capital.daphne.strategy.Strategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        // 加载配置文件
        logger.info("loading configuration file");
        String configFile;
        String projectRoot = System.getProperty("user.dir");
        if (args.length == 0) {
            configFile = projectRoot + "/config.json";
        } else {
            configFile = args[0];
        }

        ObjectMapper objectMapper = new ObjectMapper();
        AppConfig appConf;
        try {
            appConf = objectMapper.readValue(new File(configFile), AppConfig.class);
        } catch (IOException e) {
            logger.error("Can not load config file: " + e.getMessage());
            return;
        }

        logger.info("initialize database handler: mysql");
        AppConfig.Database dbConf = appConf.getDatabase();
        Db db = new Db(
                dbConf.getHost(),
                dbConf.getPort(),
                dbConf.getUser(),
                dbConf.getPassword(),
                dbConf.getDbname()
        );

        logger.info("initialize cache handler: redis");
        AppConfig.Redis redisConf = appConf.getRedis();
        JedisUtil.initializeJedisPool(
                redisConf.getHost(),
                redisConf.getPort(),
                redisConf.getPassword()
        );

        logger.info("initialize datasource handler: ibkr");
        Ibkr ibkr = new Ibkr(appConf, db);
        ibkr.connectTWS();

        logger.info("start IBKR watcher: tickers and 5s bars");
        ibkr.startTwsWatcher();

        logger.info("subscribe bar updated message to trigger signal checking logic");
        JedisPool jedisPool = JedisUtil.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    logger.info("Received message: " + message + " from channel: " + channel);
                    Signal tradeSignal = ibkr.getTradeSignal(message);
                    if (tradeSignal.isValid()) {
                        UUID uuid = UUID.randomUUID();
                        tradeSignal.setUuid(uuid.toString());
                        tradeSignal.setQuantity(tradeSignal.getQuantity());
                        if (tradeSignal.getSide().equals(Strategy.TradeActionType.BUY)) {
                            // buy时， price设置成ask price
                            tradeSignal.setPrice(tradeSignal.getAskPrice());
                        } else if (tradeSignal.getSide().equals(Strategy.TradeActionType.SELL)) {
                            // sell时， price设置成bid price
                            tradeSignal.setQuantity(-tradeSignal.getQuantity());
                            tradeSignal.setPrice(tradeSignal.getBidPrice());
                        }

                        AppConfig.SymbolConfig sc = tradeSignal.getSymbolConfig();
                        // 如果需要根据当前信号，去交易其他contract，根据rewrite信息进行改变，如根据ES的信号，下MES的单
                        AppConfig.Rewrite rewrite = sc.getRewrite();
                        if (rewrite != null) {
                            tradeSignal.setSymbol(rewrite.getSymbol());
                            tradeSignal.setSecType(rewrite.getSecType());
                            int quantity = tradeSignal.getQuantity() > 0 ? rewrite.getOrderSize() : -rewrite.getOrderSize();
                            tradeSignal.setQuantity(quantity);
                        }

                        // 记录信号
                        db.addSignal(tradeSignal);

                        // 发送下单信号
                        String traderSrvUrl = String.format(
                                "http://%s:%d/%s",
                                sc.getHttp().getHost(),
                                sc.getHttp().getPort(),
                                sc.getHttp().getPath()
                        );
                        logger.debug(traderSrvUrl + " " + tradeSignal);
                        sendSignal(traderSrvUrl, tradeSignal);

                        // 如果需要根据当前信号，同时去交易其他交易对，根据parallel进行改变，如根据SPY.STK的信号，同时下单SPY.CDF
                        AppConfig.Parallel parallel = sc.getParallel();
                        if (parallel != null) {
                            tradeSignal.setSymbol(parallel.getSymbol());
                            tradeSignal.setSecType(parallel.getSecType());
                            int quantity = tradeSignal.getQuantity() > 0 ? parallel.getOrderSize() : -parallel.getOrderSize();
                            tradeSignal.setQuantity(quantity);
                            db.addSignal(tradeSignal);
                            sendSignal(traderSrvUrl, tradeSignal);
                        }
                    }
                }
            }, "barUpdateChannel");
        } catch (Exception e) {
            logger.error("barList update failed, error:" + e.getMessage());
        }
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
            requestData.put("secType", signal.getSecType());
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