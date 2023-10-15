package capital.daphne.algorithms;

import capital.daphne.models.Signal;
import tech.tablesaw.api.Table;

public interface Algorithm {
    public Signal getSignal(Table df, int position, int maxPosition);
}
