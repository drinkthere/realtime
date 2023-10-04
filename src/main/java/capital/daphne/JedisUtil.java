package capital.daphne;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtil {
    private static JedisPool jedisPool;

    public static void initializeJedisPool(String host, int port, String password) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(5); // 设置最大连接数
        poolConfig.setMaxIdle(3);   // 设置最大空闲连接数

        jedisPool = new JedisPool(poolConfig, host, port, 10000, password); // 连接超时时间为 10 秒
    }

    public static JedisPool getJedisPool() {
        return jedisPool;
    }
}
