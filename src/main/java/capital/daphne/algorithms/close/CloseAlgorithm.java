package capital.daphne.algorithms.close;

import capital.daphne.models.Signal;
import tech.tablesaw.api.Table;

public interface CloseAlgorithm {
    public Signal getSignal(Table df, int position, int maxPosition);
}
