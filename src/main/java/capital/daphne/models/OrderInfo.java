package capital.daphne.models;

import lombok.Data;

@Data
public class OrderInfo {
    private int orderId;
    private int quantity;
    private String dateTime;
}