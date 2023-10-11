package capital.daphne;

import capital.daphne.datasource.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Db {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private String jdbcUrl;
    private String username;
    private String password;

    public Db(String host, int port, String user, String pass, String database) {
        // 连接到 MySQL 数据库
        jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", host, port, database);
        username = user;
        password = pass;
    }

    public void addSignal(Signal sig) {
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            String insertSQL = "INSERT INTO tb_signal (uuid, symbol, sec_type, bid_price, ask_price, price, wap, quantity) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement insertStatement = connection.prepareStatement(insertSQL);
            insertStatement.setString(1, sig.getUuid());
            insertStatement.setString(2, sig.getSymbol());
            insertStatement.setString(3, sig.getSecType());
            insertStatement.setDouble(4, sig.getBidPrice());
            insertStatement.setDouble(5, sig.getAskPrice());
            insertStatement.setDouble(6, sig.getPrice());
            insertStatement.setDouble(7, sig.getWap());
            insertStatement.setInt(8, sig.getQuantity());
            insertStatement.executeUpdate();
            logger.info("save signal successfully, signal: " + sig);
            connection.close();
        } catch (SQLException e) {
            logger.error("save signal failed, error:" + e.getMessage());
        }
    }
}