package capital.daphne.services;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Set;

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
            e.printStackTrace();
            logger.error(String.format("get position in redis failed, accountId=%s, symbol=%s, secType=%s error=%s",
                    accountId, symbol, secType, e.getMessage()));
        }
        return position;
    }

    public int[] calNetCallPutNum(AppConfigManager.AppConfig.TriggerOption to) {
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            String optionPositionSetKey = String.format("%s:%s:%s:OPTION_SET", to.getAccountId(), to.getSymbol(), "OPT");
            Set<String> allOptionPositionKeys = jedis.smembers(optionPositionSetKey);
            int netCall = 0;
            int netPut = 0;
            if (allOptionPositionKeys != null) {
                for (String positionKeyPrefix : allOptionPositionKeys) {
                    String[] splits = positionKeyPrefix.split(":");
                    String right = splits[1];
                    String positionKey = positionKeyPrefix + ":POSITION";
                    String pos = jedis.get(positionKey);
                    if (right.equals("Put")) {
                        netPut += Integer.parseInt(pos);
                    } else {
                        netCall += Integer.parseInt(pos);
                    }

                }
            }
            return new int[]{netCall, netPut};
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
