package capital.daphne.datasource.ibkr;

import capital.daphne.Main;
import com.ib.client.Contract;
import com.ib.client.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class PositionHandler implements IbkrController.IPositionHandler {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private HashMap<String, Integer> positionMap;

    public PositionHandler() {
        positionMap = new HashMap<>();
    }

    @Override
    public void position(String account, Contract contract, Decimal pos, double avgCost) {
        String symbol = contract.symbol();
        try {
            int position = Integer.parseInt((pos.toString()));
            positionMap.put(symbol, position);
            logger.debug(String.format("account=%s, contract=%s, position=%d, avgCost=%f", account, contract.toString(), position, avgCost));
        } catch (NumberFormatException e) {
            logger.error("format position to int error: " + e.getMessage());
        }
    }

    public int getSymbolPosition(String symbol) {
        Integer position = positionMap.get(symbol);
        if (position == null) {
            return 0;
        } else {
            return position;
        }
    }

    @Override
    public void positionEnd() {

    }
}
