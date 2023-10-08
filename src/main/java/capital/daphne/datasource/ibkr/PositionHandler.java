package capital.daphne.datasource.ibkr;

import capital.daphne.AppConfig;
import capital.daphne.Main;
import capital.daphne.utils.Utils;
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
        String secType = contract.secType().toString();
        String key = Utils.genKey(symbol, secType);
        try {
            int position = Integer.parseInt((pos.toString()));
            positionMap.put(key, position);
            logger.debug(String.format("account=%s, symbol=%s, secType=%s, position=%d, avgCost=%f", account, symbol, secType, position, avgCost));
        } catch (NumberFormatException e) {
            logger.error("format position to int error: " + e.getMessage());
        }
    }

    public int[] getSymbolPosition(AppConfig.SymbolConfig sc) {
        String key = Utils.genKey(sc.getSymbol(), sc.getSecType());
        int maxPortfolioPositions = sc.getStrategy().getMaxPortfolioPositions();

        AppConfig.Rewrite rewrite = sc.getRewrite();
        if (rewrite != null) {
            key = Utils.genKey(rewrite.getSymbol(), rewrite.getSecType());
            maxPortfolioPositions = rewrite.getMaxPortfolioPositions();
        }

        Integer position = positionMap.get(key);
        if (position == null) {
            return new int[]{0, maxPortfolioPositions};
        } else {
            return new int[]{position, maxPortfolioPositions};
        }
    }

    @Override
    public void positionEnd() {

    }
}
