package capital.daphne.services;

import capital.daphne.JedisManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class PositionSvc {

    private static final Logger logger = LoggerFactory.getLogger(PositionSvc.class);

    public int getPosition(String accountId, String symbol, String secType) {
        JedisPool jedisPool = JedisManager.getJedisPool();
        int position = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            String redisKey = accountId + ":" + symbol + ":" + secType + ":POSITION";
            String positionStr = jedis.get(redisKey);

            if (positionStr != null) {
                position = Integer.parseInt(positionStr);
            }
        } catch (Exception e) {
            logger.error(String.format("get position in redis failed, accountId=%s, symbol=%s, secType=%s error=%s",
                    accountId, symbol, secType, e.getMessage()));
        }
        return position;
    }
}
