package capital.daphne;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

public class DbManager {

    private static BasicDataSource dataSource;

    public static void initializeDbConnectionPool() {
        AppConfigManager.AppConfig appConfig = AppConfigManager.getInstance().getAppConfig();
        dataSource = new BasicDataSource();

        String jdbcUrl = String.format(
                "jdbc:mysql://%s:%d/%s",
                appConfig.getDatabase().getHost(),
                appConfig.getDatabase().getPort(),
                appConfig.getDatabase().getDbname());
        dataSource.setUrl(jdbcUrl);
        dataSource.setUsername(appConfig.getDatabase().getUser());
        dataSource.setPassword(appConfig.getDatabase().getPassword());
        dataSource.setInitialSize(appConfig.getDatabase().getInitialConnectionsSize()); // 初始连接数
        dataSource.setMaxTotal(appConfig.getDatabase().getMaxConnectionsTotal()); // 最大连接数
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }
}
