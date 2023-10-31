package capital.daphne.models;

import lombok.Data;

@Data
public class WapMaxMin {
    double maxPriceSinceLastOrder;
    double minPriceSinceLastOrder;
}