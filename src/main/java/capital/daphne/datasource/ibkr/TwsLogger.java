package capital.daphne.datasource.ibkr;

import com.ib.controller.ApiConnection;


public class TwsLogger implements ApiConnection.ILogger {

    @Override
    public void log(String s) {
        System.out.println(s);
    }
}
