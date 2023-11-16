package dma;

import capital.daphne.AppConfigManager;
import capital.daphne.JedisManager;
import capital.daphne.algorithms.DMA;
import capital.daphne.models.BarInfo;
import capital.daphne.models.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import tech.tablesaw.api.Table;
import testmodels.Bar;
import testmodels.Sig;
import testutils.TestUtils;

import java.util.ArrayList;
import java.util.List;

public class DMATest {
    private static final Logger logger = LoggerFactory.getLogger(DMATest.class);
    private AppConfigManager.AppConfig.AlgorithmConfig ac;


    @Test
    public void testDMA() {
        //!!!!!!!!!!!!!!!!!!!!!! 记得修改下MACDSignal/MACDZERO的计算时间的代码，把对应的行注释掉
        logger.info("initialize cache handler: redis");
        JedisManager.initializeJedisPool();

        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        List<AppConfigManager.AppConfig.AlgorithmConfig> algorithms = appConfig.getAlgorithms();
        ac = algorithms.get(0);
        DMA dma = new DMA(ac);


        String currentDirectory = System.getProperty("user.dir");
        // 读取csv文件，加载List<Bar>
        List<Bar> bars = TestUtils.loadCsv(currentDirectory + "/src/test/java/sma/spy_2023-09-27--2023-09-29.csv");
        List<BarInfo> barList = new ArrayList<>();
        int maxBarListSize = 1500;
        int position = 0;
        int maxPosition = ac.getMaxPortfolioPositions();
        String processingDate = null;
        List<Sig> result = new ArrayList<>();
        for (int i = 0; i < bars.size(); i++) {
            Bar bar = bars.get(i);
            String[] splits = bar.getDate().split(" ");
            String date = splits[0];

            if (processingDate == null || !processingDate.equals(date)) {
                processingDate = date;
                barList.clear();
            }

            BarInfo barInfo = processBar(bar);
            if (barInfo == null) {
                continue;
            }
            // System.out.println(barInfo.getVwap());
            barList.add(barInfo);
            if (barList.size() > maxBarListSize) {
                // 如果超过最大值，移除最早加入barList的数据
                barList.remove(0);
            }

            if (barList.size() < ac.getNumStatsBars()) {
                continue;
            }

            // 生成dataframe
            Table df = TestUtils.getTable(barList, maxBarListSize);

            // 获取信号
            Signal signal = dma.getSignal(df, position, maxPosition);
            if (signal != null && signal.isValid()) {
                position += signal.getQuantity();
                Sig sig = new Sig();
                sig.setIndex(i);
                sig.setQuantity(signal.getQuantity());
                sig.setOrderType(signal.getOrderType().name());
                result.add(sig);

                // update order list in redis
                // updateOrdersInRedis(ac.getSymbol(), ac.getSecType(), i, signal.getQuantity(), "OPEN", row);
            }
        }
        System.out.println(result.toString());
    }

    public BarInfo processBar(Bar bar) {
        // 获取wap信息
        double wap = bar.getVwap();
        double threshold = wap;
        // 如果volume <= 0 （MIDPOINT 或 部分TRADES), 这个时候用TWAP;
        if (bar.getVolume() <= 0) {
            wap = (bar.getOpen() + bar.getHigh() + bar.getLow() + bar.getClose()) / 4;
            threshold = wap;
        }

        if (wap / threshold >= 1.001 || wap / threshold <= 0.999) {
            return null;
        }

        BarInfo barInfo = new BarInfo();
        try {
            barInfo.setDate(bar.getDate());
            barInfo.setVwap(wap);
            barInfo.setHigh(bar.getHigh());
            barInfo.setLow(bar.getLow());
            barInfo.setOpen(bar.getOpen());
            barInfo.setClose(bar.getClose());
            barInfo.setVolatility(0);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return barInfo;
    }
}
