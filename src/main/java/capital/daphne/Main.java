package capital.daphne;

import capital.daphne.models.Signal;
import capital.daphne.services.BarSvc;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static BarSvc barSvc;

    public static void main(String[] args) {
        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();

        barSvc = new BarSvc();

        logger.info("initialize database handler: mysql");
        DbManager.initializeDbConnectionPool();

        logger.info("initialize cache handler: redis");
        JedisManager.initializeJedisPool();

        logger.info("initialize signal service");
        SignalSvc signalSvc = new SignalSvc(appConfig.getAlgorithms());

        ScheduledExecutorService clearRedisCacheExecutor = Executors.newScheduledThreadPool(1);
        scheduleClearRedisCache(clearRedisCacheExecutor);


        JedisPool jedisPool = JedisManager.getPubsubPool();
        try (Jedis jedis = jedisPool.getResource()) {
            // 监听bar更新的消息，然后根据配置中的algorithm来进行判断和处理，如果有信号，就给trader模块发送信号
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    logger.info("Received message: " + message + " from channel: " + channel);
                    String[] strings = Utils.parseKey(message);
                    String symbol = strings[0];
                    String secType = strings[1];

                    // 在algorithms里面，过滤出涉及到这个股票的algorithm
                    List<AppConfigManager.AppConfig.AlgorithmConfig> matchedAlgorithms = appConfig.getAlgorithms().stream()
                            .filter(algorithm -> symbol.equals(algorithm.getSymbol()) && secType.equals(algorithm.getSecType()))
                            .collect(Collectors.toList());
                    if (matchedAlgorithms.size() == 0) {
                        logger.error("no matched algorithms to process");
                        return;
                    }

                    // 获取wapList
                    List<String> wapList = barSvc.getWapList(Utils.genKey(symbol, secType));

                    // 根据matchedAlgorithms，开启对应的线程来并行处理
                    int numThreads = matchedAlgorithms.size();
                    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

                    for (AppConfigManager.AppConfig.AlgorithmConfig ac : matchedAlgorithms) {
                        executor.submit(() -> {
                            try {
                                // 初始化ema的值
                                barSvc.initEma(ac.getAccountId(), ac.getSymbol(), ac.getSecType(), wapList, ac.getNumStatsBars());

                                // 如果当前股票已经有信号在处理中，就跳过
                                String inProgressKey = String.format("%s:%s:%s:IN_PROGRESS", ac.getAccountId(), ac.getSymbol(), ac.getSecType());
                                boolean inProgress = Utils.isInProgress(inProgressKey);
                                if (inProgress) {
                                    logger.warn(String.format("%s Order is in progressing, won't trigger signal this time", inProgressKey));
                                    return;
                                }

                                // 获取信号
                                Signal tradeSignal = signalSvc.getTradeSignal(ac, wapList);
                                if (tradeSignal != null && tradeSignal.isValid()) {
                                    // 记录信号
                                    signalSvc.saveSignal(tradeSignal);

                                    // 发送下单信号
                                    signalSvc.sendSignal(tradeSignal);

                                    // 加锁，60s过期，订单成交也会解锁
                                    Utils.setInProgress(inProgressKey);

                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                    executor.shutdown();
                }
            }, "barUpdateChannel");
        } catch (Exception e) {
            logger.error("handling subscription message failed, error:" + e.getMessage());
        }
    }

    private static void scheduleClearRedisCache(ScheduledExecutorService executor) {
        executor.scheduleAtFixedRate(() -> {
            // 当结束交易时，清除ema cache
            AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
            List<AppConfigManager.AppConfig.AlgorithmConfig> algorithms = appConfig.getAlgorithms();
            for (AppConfigManager.AppConfig.AlgorithmConfig ac : algorithms) {
                if (Utils.isMarketClose(ac.getSymbol(), ac.getSecType(), Utils.genUsDateTimeNow())) {
                    // 交易结束期间，清除缓存
                    barSvc.clearEma(ac.getAccountId(), ac.getSymbol(), ac.getSecType());
                }
            }
        }, 0, 5, TimeUnit.MINUTES);
        logger.info("connection monitor task started");
    }
}