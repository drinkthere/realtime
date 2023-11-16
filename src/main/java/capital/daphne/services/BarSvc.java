package capital.daphne.services;

import capital.daphne.JedisManager;
import capital.daphne.models.BarInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.util.List;

public class BarSvc {
    private static final Logger logger = LoggerFactory.getLogger(BarSvc.class);

    /**
     * 通过datasource服务获取BAR_LIST写入Redis，这里从Redis读取数据
     * 如果还没有达到所需的最少bar数量，就返回null，避免判断是否要给出signal的时候因为数据缺失出问题
     * 返回数据前，需要计算出prev_vwap
     */
    public Table getDataTable(String key, int minBarNum) {
        JedisPool jedisPool = JedisManager.getJedisPool();
        try (Jedis jedis = jedisPool.getResource()) {
            // 数据只与symbol和secType油管，和accountId无关
            String redisKey = key + ":BAR_LIST";
            String storedBarListJson = jedis.get(redisKey);
            if (storedBarListJson != null) {
                ObjectMapper objectMapper = new ObjectMapper();
                List<BarInfo> barList = objectMapper.readValue(storedBarListJson, new TypeReference<>() {
                });

                if (barList.size() < minBarNum) {
                    logger.info(String.format("barList is not ready, minBarNum=%d, currBarNum=%d", minBarNum, barList.size()));
                    return null;
                }

                Table dataframe = Table.create("IBKR Bar Dataframe");
                // 添加列到表格
                dataframe.addColumns(
                        StringColumn.create("date_us"),
                        DoubleColumn.create("vwap"),
                        DoubleColumn.create("open"),
                        DoubleColumn.create("close"),
                        DoubleColumn.create("high"),
                        DoubleColumn.create("low"),
                        DoubleColumn.create("volatility"));

                List<BarInfo> validBarList = barList.subList(barList.size() - minBarNum, barList.size());
                for (BarInfo bar : validBarList) {
                    dataframe.stringColumn("date_us").append(bar.getDate());
                    dataframe.doubleColumn("vwap").append(bar.getVwap());
                    dataframe.doubleColumn("open").append(bar.getOpen());
                    dataframe.doubleColumn("high").append(bar.getHigh());
                    dataframe.doubleColumn("low").append(bar.getLow());
                    dataframe.doubleColumn("close").append(bar.getClose());
                    dataframe.doubleColumn("volatility").append(bar.getVolatility());
                }
                DoubleColumn prevVWapColumn = dataframe.doubleColumn("vwap").lag(1);
                dataframe.addColumns(prevVWapColumn.setName("prev_vwap"));
                return dataframe;
            } else {
                logger.info(key + " barList is empty");
                return null;
            }
        } catch (Exception e) {
            logger.error("getDataTable, error:" + e.getMessage());
            return null;
        }
    }
}
