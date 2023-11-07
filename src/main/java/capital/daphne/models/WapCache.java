package capital.daphne.models;

import lombok.Data;

@Data
public class WapCache {
    private double maxWap;
    private double minWap;

    public WapCache() {
        maxWap = Double.MIN_VALUE;
        minWap = Double.MAX_VALUE;
    }
}