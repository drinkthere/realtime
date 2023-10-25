package capital.daphne.models;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class ActionInfo {
    private Signal.TradeActionType action;
    private LocalDateTime dateTime;
}