package capital.daphne.datasource.ibkr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class ConnectHandler implements IbkrController.IConnectionHandler {
    private static final Logger logger = LoggerFactory.getLogger(ConnectHandler.class);

    @Override
    public void connected() {
        logger.info("IBKR TWS connected");
    }

    @Override
    public void disconnected() {
        logger.warn("IBKR TWS stopped");
    }

    @Override
    public void accountList(List<String> list) {
        logger.info("IBKR account list" + list.toString());
    }

    @Override
    public void error(Exception e) {
        logger.error(e.getMessage());
    }

    @Override
    public void message(int status, int code, String message, String s1) {
        logger.info(String.format("status=%d, code=%d, message=%s, s1=%s", status, code, message, s1));
    }

    @Override
    public void show(String s) {
        logger.info(s);
    }
}
