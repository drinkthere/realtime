package capital.daphne;

import capital.daphne.models.OrderInfo;
import capital.daphne.models.Signal;
import capital.daphne.models.WapCache;
import capital.daphne.services.BarSvc;
import capital.daphne.services.PositionSvc;
import capital.daphne.services.SignalSvc;
import capital.daphne.utils.Utils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ib.client.TickType;
import com.ib.client.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    private static BarSvc barSvc;
    private static PositionSvc positionSvc;

    private static SignalSvc signalSvc;

    private static AppConfigManager.AppConfig appConfig;

    public static void main(String[] args) {
        appConfig = AppConfigManager.getInstance().getAppConfig();

        barSvc = new BarSvc();
        positionSvc = new PositionSvc();

        logger.info("initialize database handler: mysql");
        DbManager.initializeDbConnectionPool();

        logger.info("initialize cache handler: redis");
        JedisManager.initializeJedisPool();

        logger.info("initialize signal service");
        signalSvc = new SignalSvc(appConfig.getAlgorithms());

        logger.info("start bar data update runner");
        startDataUpdateThread();

        logger.info("start algorithm runner");
        startAlgorithmThread();
    }


    private static void startDataUpdateThread() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> startDataUpdate());
    }

    private static void startDataUpdate() {
        JedisPool jedisPool = JedisManager.getPubsubPool();
        try (Jedis jedis = jedisPool.getResource()) {
            // 监听bar更新的消息，然后根据配置中的algorithm来进行判断和处理，如果有信号，就给trader模块发送信号
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    logger.info("Received bar message: " + message + " from channel: " + channel);
                    String[] strings = Utils.parseKey(message);
                    String symbol = strings[0];
                    String secType = strings[1];

                    // 在algorithms里面，过滤出涉及到这个股票的algorithm
                    List<AppConfigManager.AppConfig.AlgorithmConfig> matchedAlgorithms = appConfig.getAlgorithms().stream()
                            .filter(algorithm -> symbol.equals(algorithm.getSymbol()) && secType.equals(algorithm.getSecType()))
                            .collect(Collectors.toList());
                    if (matchedAlgorithms.size() == 0) {
                        logger.error(String.format("%s %s no matched algorithms to process", symbol, secType));
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
                                String key = ac.getAccountId() + ":" + ac.getSymbol() + ":" + ac.getSecType();
                                Double prevEma = barSvc.getEma(key);
                                if (prevEma <= 0) {
                                    prevEma = barSvc.initEma(key, wapList, ac.getNumStatsBars());
                                }

                                // 计算当前标的的ema并更新
                                int period = ac.getNumStatsBars();
                                double multiplier = 2.0 / (period + 1);
                                int lastIndex = wapList.size() - 1;
                                Double vwap = Double.parseDouble(wapList.get(lastIndex));
                                Double ema = vwap * multiplier + prevEma * (1 - multiplier);
                                barSvc.setEma(key, ema);
                                barSvc.saveEmaToDb(key, ac.getName() + period, ema);

                                // 计算当前标的的volatility, volatilityMultiplier
                                double volatility = barSvc.calVolatility(ac, wapList);
                                double volatilityMultiplier = Utils.calToVolatilityMultiplier(ac.getVolatilityA(), ac.getVolatilityB(), ac.getVolatilityC(), volatility);
                                barSvc.updateVolatilityInRedis(key, volatility + "|" + volatilityMultiplier);

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


    private static void startAlgorithmThread() {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> startAlgorithm());
    }

    private static void startAlgorithm() {
        JedisPool jedisPool = JedisManager.getPubsubPool();
        try (Jedis jedis = jedisPool.getResource()) {
            // 监听bar更新的消息，然后根据配置中的algorithm来进行判断和处理，如果有信号，就给trader模块发送信号
            jedis.subscribe(new JedisPubSub() {
                @Override
                public void onMessage(String channel, String key) {
                    logger.info("Received tick message: " + key + " from channel: " + channel);
                    String[] strings = Utils.parseKey(key);
                    String symbol = strings[0];
                    String secType = strings[1];

                    // 获取bid ask price
                    double bidPrice = Utils.getTickerPrice(key, TickType.BID);
                    double askPrice = Utils.getTickerPrice(key, TickType.ASK);

                    if (bidPrice == 0.0 || askPrice == 0.0) {
                        logger.error(String.format("Bid price or ask price is invalid, symbol=%s, secType=%s, bidPrice=%f, askPrice=%f", symbol, secType, bidPrice, askPrice));
                        return;
                    }

                    // 在algorithms里面，过滤出涉及到这个股票的algorithm
                    List<AppConfigManager.AppConfig.AlgorithmConfig> matchedAlgorithms = appConfig.getAlgorithms().stream()
                            .filter(algorithm -> symbol.equals(algorithm.getSymbol()) && secType.equals(algorithm.getSecType()))
                            .collect(Collectors.toList());
                    if (matchedAlgorithms.size() == 0) {
                        logger.error(String.format("%s %s no matched algorithms to process", symbol, secType));
                        return;
                    }

                    // 根据matchedAlgorithms，开启对应的线程来并行处理
                    int numThreads = matchedAlgorithms.size();
                    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                    for (AppConfigManager.AppConfig.AlgorithmConfig ac : matchedAlgorithms) {
                        executor.submit(() -> {
                            try {
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

                                // 获取信号
                                Signal tradeSignal = signalSvc.getTradeSignal(ac, bidPrice, askPrice);
                                if (tradeSignal != null && tradeSignal.isValid()) {
                                    // if (tradeSignal.getOrderType().equals(Signal.OrderType.OPEN) && ac.getDelayMs() > 0) {
                                    //  Thread.sleep(ac.getDelayMs());
                                    // }

                                    if (!ac.isOnlyTriggerOption()) {
                                        // 如果不是只发送option数据，这里就把信号标的也发送了
                                        // 记录信号
                                        signalSvc.saveSignal(tradeSignal);

                                        // 发送下单信号
                                        signalSvc.sendSignal(tradeSignal);

                                        // 加锁，60s过期，订单成交也会解锁
                                        Utils.setInProgress(inProgressKey);
                                    } else {
                                        // 不真实发送信号，但是要更新下仓位，便于后续信号判断
                                        String accountId = tradeSignal.getAccountId();

                                        int quantity = tradeSignal.getQuantity();
                                        String orderType = tradeSignal.getOrderType().name();
                                        String strategy = tradeSignal.getBenchmarkColumn();
                                        updateOrdersInRedis(accountId, symbol, secType, 0, quantity, orderType, strategy);
                                        updateWapMaxMinInRedis(accountId, symbol, secType, orderType, tradeSignal.getWap());
                                        positionSvc.updatePosition(tradeSignal.getAccountId(), tradeSignal.getSymbol(), tradeSignal.getSecType(), quantity);
                                    }

                                    // 如果配置了option，就去下期权单
                                    AppConfigManager.AppConfig.TriggerOption to = ac.getTriggerOption();
                                    if (to != null) {
                                        inProgressKey = String.format("%s:%s:%s:IN_PROGRESS", ac.getAccountId(), symbol, Types.SecType.OPT.name());
                                        inProgress = Utils.isInProgress(inProgressKey);
                                        if (inProgress) {
                                            logger.warn(String.format("%s Order is in progressing, won't trigger signal this time", inProgressKey));
                                            return;
                                        }

                                        // 额外触发option的下单信号
                                        signalSvc.triggerOptionSignal(tradeSignal, ac.getTriggerOption());
                                        Utils.setInProgress(inProgressKey);
                                    }

                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        });
                    }
                    executor.shutdown();
                }
            }, "tickerUpdate");
        } catch (Exception e) {
            logger.error("handling subscription message failed, error:" + e.getMessage());
        }
    }

    private static void updateOrdersInRedis(String accountId, String symbol, String secType, int orderId, int quantity, String orderType, String strategy) {
        String lastActionInfoKey = String.format("%s:%s:%s:%s:LAST_ACTION", accountId, symbol, secType, strategy);
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = String.format("%s:%s:%s:ORDER_LIST", accountId, symbol, secType);
            if (orderType.equals("CLOSE")) {
                // close信号来时，清除队列中的数据
                jedis.del(redisKey);
                jedis.del(lastActionInfoKey);
            } else if (orderType.equals("OPEN")) {
                // open信号来时，添加数据进入队列
                List<OrderInfo> orderList = new ArrayList<>();
                String storedOrderListJson = jedis.get(redisKey);
                if (storedOrderListJson != null) {
                    ObjectMapper objectMapper = new ObjectMapper();
                    orderList = objectMapper.readValue(storedOrderListJson, new TypeReference<>() {
                    });
                }
                OrderInfo orderInfo = new OrderInfo();
                orderInfo.setOrderId(orderId);
                orderInfo.setQuantity(quantity);
                orderInfo.setDateTime(LocalDateTime.now().truncatedTo(ChronoUnit.MILLIS).toString());
                orderList.add(orderInfo);
                ObjectMapper objectMapper = new ObjectMapper();
                String orderListJson = objectMapper.writeValueAsString(orderList);
                jedis.set(redisKey, orderListJson);
                logger.warn(redisKey + " update order in redis successfully.");

                // 更新策略的lastActionInfo，realtime模块需要这个信息来判断是否给出信号
                String action = quantity > 0 ? "BUY" : "SELL";

                ZonedDateTime usEasternTime = ZonedDateTime.now()
                        .withZoneSameInstant(java.time.ZoneId.of("US/Eastern"));
                DateTimeFormatter usFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
                String now = usEasternTime.format(usFormatter);
                jedis.set(lastActionInfoKey, action + "|" + now);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("%s %s %s %s update position in redis failed", accountId, symbol, secType, strategy));
        }
    }

    private static void updateWapMaxMinInRedis(String accountId, String symbol, String secType, String orderType, double filledPrice) {
        if (orderType.equals("CLOSE")) {
            return;
        }
        // 只有OPEN订单重置wapMaxMin，用于计算trailing stop
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            WapCache wapMaxMin = new WapCache();
            wapMaxMin.setMinWap(filledPrice);
            wapMaxMin.setMaxWap(filledPrice);
            ObjectMapper maxMinMapper = new ObjectMapper();
            String wapMaxMinJson = maxMinMapper.writeValueAsString(wapMaxMin);
            String maxMinKey = String.format("%s:%s:MAX_MIN_WAP", symbol, secType);
            jedis.set(maxMinKey, wapMaxMinJson);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.format("%s %s %s %s update wap max min in redis failed", accountId, symbol, secType, filledPrice));
        }
    }
}