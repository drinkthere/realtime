package capital.daphne.models;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class TradingHours {
    LocalDateTime startTime;
    LocalDateTime endTime;
    boolean closed;

    public TradingHours(LocalDateTime startTime, LocalDateTime endTime, boolean isClosed) {
        this.startTime = startTime;
        this.endTime = endTime;
        this.closed = isClosed;
    }
}
