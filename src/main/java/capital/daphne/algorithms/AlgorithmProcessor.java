package capital.daphne.algorithms;

import capital.daphne.models.Signal;
import tech.tablesaw.api.Table;

import java.util.List;

public interface AlgorithmProcessor {
    public Signal getSignal(Table df, int position, int maxPosition);

    public List<Signal> getGTDSignals(Table df, int position, int maxPosition);
}
