package capital.daphne.models;

import lombok.Data;

import java.time.LocalDateTime;

@Data
public class OrderInfo {
    private int orderId;
    private int quantity;
    private LocalDateTime dateTime;
}