package capital.daphne.datasource.polygon;

import io.polygon.kotlin.sdk.websocket.*;
import lombok.Data;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
class TradeInfo {
    private float price;
}

@Data
class Ticker {
    private float bidPrice;
    private int bidPriceUpdateTs;
    private float askPrice;
    private int askPriceUpdateTs;
}

public class Polygon {
    // 最新的ticker数据，以及对应的更新时间
    private float bidPrice;
    private int bidPrideUpdateTs;
    private float askPrice;
    private int askPriceUpdateTs;

    private List<TradeInfo> tradeInfoList;

    private String apiKey;
    // 每secondsPerBar秒时间的数据生成一个bar
    private int secPerBar;
    private String symbol;

    private PolygonWebSocketClient wsClient;
    List<PolygonWebSocketSubscription> subscriptions;

    public Polygon(String apiKey, String symbol, int secPerBar) {
        tradeInfoList = new ArrayList<>();
        this.apiKey = apiKey;
        this.secPerBar = secPerBar;
        this.symbol = symbol;

    }

    private void handleMessage(PolygonWebSocketMessage message) {
        if (message instanceof PolygonWebSocketMessage.RawMessage) {
            System.out.println(new String(((PolygonWebSocketMessage.RawMessage) message).getData()));
        } else {

            System.out.println(message.toString());
        }
        // handle quote message, 更新ticker信息

        // handle trade message，更新tradeInfoList
    }

    private void handleQuoteMessage() {
        // 根据quote消息，更新bid、ask以及update timestamp信息
    }

    private void handleTradeEvent() {
        synchronized (tradeInfoList) {
            // 将trade消息添加到tradeInfoList中
            // 这里无脑去添加，主线程生成bar之后，会调用removeExpiredTradeInfo来删除用过的信息
        }
    }

    public List<TradeInfo> getTradeInfoList() {
        return tradeInfoList;
    }

    public void removeExpiredTradeInfo(LocalDateTime datetime) {
        // 删除tradeInfoList中，< datetime的数据
        synchronized (tradeInfoList) {

        }
    }

    // 返回ticker数据
    public Ticker getTickerInfo() {
        Ticker ticker = new Ticker();
        ticker.setAskPrice(askPrice);
        ticker.setBidPriceUpdateTs(bidPrideUpdateTs);
        ticker.setBidPrice(bidPrice);
        ticker.setAskPriceUpdateTs(askPriceUpdateTs);
        return ticker;
    }

    public void run() {
        // 链接websocket服务
        // 注册监听事件，ticker和trade
        wsClient = new PolygonWebSocketClient(
                apiKey,
                PolygonWebSocketCluster.Stocks,
                new DefaultPolygonWebSocketListener() {
                    @Override
                    public void onReceive(@NotNull PolygonWebSocketClient client, @NotNull PolygonWebSocketMessage message) {
                        handleMessage(message);
                    }

                    @Override
                    public void onError(@NotNull PolygonWebSocketClient client, @NotNull Throwable error) {
                        System.out.println("Error in websocket");
                        error.printStackTrace();
                    }
                });
        try {
            wsClient.connectBlocking();
            List<PolygonWebSocketSubscription> subs = new ArrayList<>();
            subs.add(new PolygonWebSocketSubscription(PolygonWebSocketChannel.Stocks.Trades.INSTANCE, "SPY"));
            subs.add(new PolygonWebSocketSubscription(PolygonWebSocketChannel.Stocks.Quotes.INSTANCE, "SPY"));
            wsClient.subscribeBlocking(subs);

            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            wsClient.unsubscribeBlocking(subs);
            wsClient.disconnectBlocking();


        } catch (Exception e) {
            e.printStackTrace();
            disconnect();
        }
    }

    private void disconnect() {
        if (wsClient != null) {
            wsClient.unsubscribeBlocking(subscriptions);
            wsClient.disconnectBlocking();
        }
    }
}
