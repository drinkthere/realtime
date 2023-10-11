package capital.daphne.service;

import capital.daphne.JedisUtil;
import com.ib.client.TickType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;


public class TickerSvc {
    private static final Logger logger = LoggerFactory.getLogger(TickerSvc.class);

    public TickerSvc() {

    }

    public double getPrice(String key, TickType tickType) {
        JedisPool jedisPool = JedisUtil.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = key + "." + tickType;
            String price = jedis.get(redisKey);
            return Double.parseDouble(price);
        } catch (Exception e) {
            logger.error(String.format("get %s %s failed, error:%s", key, tickType, e.getMessage()));
            return 0.0;
        }
    }
}
