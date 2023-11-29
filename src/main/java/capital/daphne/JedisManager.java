package capital.daphne;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisManager {
    private static JedisPool jedisPool;

    private static JedisPool pubsubPool;

    public static void initializeJedisPool() {
        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20); // 设置最大连接数
        poolConfig.setMaxIdle(20);   // 设置最大空闲连接数
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWaitMillis(3000);
        jedisPool = new JedisPool(
                poolConfig,
                appConfig.getRedis().getHost(),
                appConfig.getRedis().getPort(),
                2000,
                appConfig.getRedis().getPassword()); // 连接超时时间为 2 秒
    }

    public static JedisPool getJedisPool() {
        return jedisPool;
    }
}
