package capital.daphne;

import capital.daphne.models.Signal;
import capital.daphne.services.SignalSvc;
import capital.daphne.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();

        logger.info("initialize database handler: mysql");
        DbManager.initializeDbConnectionPool();

        logger.info("initialize cache handler: redis");
        JedisManager.initializeJedisPool();

        logger.info("initialize signal service");
        SignalSvc signalSvc = new SignalSvc(appConfig.getAlgorithms());

        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    logger.info("Received message: " + message + " from channel: " + channel);
                    String[] strings = Utils.parseKey(message);
                    String symbol = strings[0];
                    String secType = strings[1];

                    // algorithms里面，涉及到这个标的的algorithm
                    List<AppConfigManager.AppConfig.AlgorithmConfig> matchedAlgorithms = appConfig.getAlgorithms().stream()
                            .filter(algorithm -> symbol.equals(algorithm.getSymbol()) && secType.equals(algorithm.getSecType()))
                            .collect(Collectors.toList());

                    int numThreads = matchedAlgorithms.size();
                    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

                    for (AppConfigManager.AppConfig.AlgorithmConfig ac : matchedAlgorithms) {
                        executor.submit(() -> {
                            Signal tradeSignal = signalSvc.getTradeSignal(ac);
                            if (tradeSignal != null && tradeSignal.isValid()) {
                                // 记录信号
                                signalSvc.saveSignal(tradeSignal);

                                // 发送下单信号
                                signalSvc.sendSignal(tradeSignal);
                            }
                        });
                    }
                    executor.shutdown();
                }
            }, "barUpdateChannel");
        } catch (Exception e) {
            logger.error("barList update failed, error:" + e.getMessage());
        }
    }
}