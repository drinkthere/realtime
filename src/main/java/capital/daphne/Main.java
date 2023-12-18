package capital.daphne;

import capital.daphne.models.Signal;
import capital.daphne.services.BarSvc;
import capital.daphne.services.SignalSvc;
import capital.daphne.utils.Utils;
import com.ib.client.TickType;
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

        JedisPool jedisPool = JedisManager.getJedisPool();
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
                        logger.warn(String.format("%s %s no matched algorithms to process", symbol, secType));
                        return;
                    }

                    // 获取wapList
                    List<String> wapList = barSvc.getWapList(Utils.genKey(symbol, secType));

                    // 获取bid ask price
                    String key = Utils.genKey(symbol, secType);
                    double bidPrice = Utils.getTickerPrice(key, TickType.BID);
                    double askPrice = Utils.getTickerPrice(key, TickType.ASK);
                    if (bidPrice == 0.0 || askPrice == 0.0) {
                        logger.warn(String.format("Bid price or ask price is invalid, symbol=%s, bidPrice=%f, askPrice=%f", symbol, bidPrice, askPrice));
                        return;
                    }


                    // 根据matchedAlgorithms，开启对应的线程来并行处理
                    int numThreads = matchedAlgorithms.size();
                    ExecutorService executor = Executors.newFixedThreadPool(numThreads);

                    for (AppConfigManager.AppConfig.AlgorithmConfig ac : matchedAlgorithms) {
                        executor.submit(() -> {
                            try {

                                // 计算当前标的的volatility
                                double volatility = barSvc.calVolatility(ac, wapList);

                                // 获取信号
                                Signal tradeSignal = signalSvc.getTradeSignal(ac, volatility, bidPrice, askPrice);
                                if (tradeSignal != null && tradeSignal.isValid()) {
                                    // 之所以把判断条件放在这里，是因为有些交易的benchmark（如EMA）对历史数据是有依赖的
                                    // 因此无论如何都调用一下getTradingSingal，把对应的benchmark值给计算出来

                                    // 当前股票已经有交易在进行
                                    String inProgressKey = String.format("%s:%s:%s:IN_PROGRESS", ac.getAccountId(), ac.getSymbol(), ac.getSecType());
                                    boolean inProgress = Utils.isInProgress(inProgressKey);
                                    if (inProgress) {
                                        logger.warn(String.format("%s Order is in progressing, won't trigger signal this time", inProgressKey));
                                        return;
                                    }

                                    // 当前是否是可交易时间
                                    boolean isTradingNow = Utils.isTradingNow(symbol, secType, Utils.genUsDateTimeNow(), ac.getStartTradingAfterOpenMarketSeconds());
                                    if (!isTradingNow) {
                                        logger.info(String.format("account=%s, symbol=%s, secType=%s, is not trading now", ac.getAccountId(), symbol, secType));
                                        return;
                                    }

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
            logger.warn("handling subscription message failed, error:" + e.getMessage());
        }
    }
}