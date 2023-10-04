package capital.daphne;

import capital.daphne.datasource.Signal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

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
            String insertSQL = "INSERT INTO tb_signal (uuid, symbol, bid_price, ask_price, price, wap, quantity) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement insertStatement = connection.prepareStatement(insertSQL);
            insertStatement.setString(1, sig.getUuid());
            insertStatement.setString(2, sig.getSymbol());
            insertStatement.setDouble(3, sig.getBidPrice());
            insertStatement.setDouble(4, sig.getAskPrice());
            insertStatement.setDouble(5, sig.getPrice());
            insertStatement.setDouble(6, sig.getWap());
            insertStatement.setInt(7, sig.getQuantity());
            insertStatement.executeUpdate();
            logger.info("save signal successfully, signal: " + sig);
            connection.close();
        } catch (SQLException e) {
            logger.error("save signal failed, error:" + e.getMessage());
        }
    }

    public Map<String, double[]> loadWapCache(List<AppConfig.SymbolItem> symbolList) {
        HashMap<String, double[]> symbolWapMap = new HashMap<>();
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            for (AppConfig.SymbolItem symbolItem : symbolList) {
                String symbol = symbolItem.getSymbol();
                double[] wapArr = loadSymbolWapCache(connection, symbol);
                symbolWapMap.put(symbol, wapArr);
            }
            connection.close();
        } catch (SQLException e) {
            logger.error("load wap cache failed, error:" + e.getMessage());
        }
        return symbolWapMap;
    }

    private double[] loadSymbolWapCache(Connection connection, String symbol) {
        double[] wapArr = {0.0, 0.0, 0.0, 0.0};
        // 获取当前日期（美东时间）
        TimeZone timeZone = TimeZone.getTimeZone("US/Eastern");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(timeZone);
        Date currentDateTime = new Date();
        String currDate = dateFormat.format(currentDateTime);

        // 查询当前日期的max_wap和min_wap数据
        double[] currDateWapArr = getWap(connection, symbol, currDate);
        if (currDateWapArr == null) {
            currDateWapArr = new double[]{0.0, 0.0};
        }

        // 获取上一个交易日的max_wap和min_wap，最多向前回溯5个交易日
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(currentDateTime);
        double prevMaxWap = 0.0;
        double prevMinWap = 0.0;
        for (int i = 1; i <= 5; i++) {
            calendar.add(Calendar.DAY_OF_YEAR, -i);
            Date prevDateTime = calendar.getTime();
            String prevDate = dateFormat.format(prevDateTime);
            double[] prevDateWapArr = getWap(connection, symbol, prevDate);
            if (prevDateWapArr != null && prevDateWapArr[0] != 0 && prevDateWapArr[1] != 0) {
                prevMaxWap = prevDateWapArr[0];
                prevMinWap = prevDateWapArr[1];
            }
        }
        wapArr[0] = prevMaxWap;
        wapArr[1] = prevMinWap;
        wapArr[2] = currDateWapArr[0];
        wapArr[3] = currDateWapArr[1];

        return wapArr;
    }

    public void updateWapCache(String symbol, double maxWap, double minWap) {
        try {
            Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
            String currUsDate = getCurrUsDate();
            double[] wapArr = getWap(connection, symbol, currUsDate);
            if (wapArr == null) {
                // insert data
                String insertSQL = "INSERT INTO tb_wap (symbol, max_wap, min_wap, us_date) VALUES (?, ?, ?, ?)";
                PreparedStatement insertStatement = connection.prepareStatement(insertSQL);
                insertStatement.setString(1, symbol);
                insertStatement.setDouble(2, maxWap);
                insertStatement.setDouble(3, minWap);
                insertStatement.setString(4, currUsDate);
                int rowCount = insertStatement.executeUpdate();
                if (rowCount > 0) {
                    System.out.println("wap cache insert successfully.");
                } else {
                    System.out.println("wap cache insert failed.");
                }
            } else {
                String sql = "UPDATE tb_wap SET max_wap = ?, min_wap = ? WHERE symbol = ? AND us_date = ?";
                try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    preparedStatement.setDouble(1, maxWap);
                    preparedStatement.setDouble(2, minWap);
                    preparedStatement.setString(3, symbol);
                    preparedStatement.setString(4, currUsDate);

                    int rowCount = preparedStatement.executeUpdate();
                    if (rowCount > 0) {
                        logger.info("wap cache update successfully。");
                    } else {
                        logger.info("wap cache update failed。");
                    }
                }
            }
            connection.close();
        } catch (SQLException e) {
            logger.error("save signal failed, error:" + e.getMessage());
        }
    }

    private String getCurrUsDate() {
        TimeZone timeZone = TimeZone.getTimeZone("US/Eastern");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        dateFormat.setTimeZone(timeZone);
        Date currentDateTime = new Date();
        return dateFormat.format(currentDateTime);
    }

    private double[] getWap(Connection connection, String symbol, String usDate) {
        try {
            String sql = "SELECT max_wap, min_wap FROM tb_wap WHERE symbol = ? AND us_date = ? LIMIT 1";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1, symbol);
            preparedStatement.setString(2, usDate);
            ResultSet resultSet = preparedStatement.executeQuery();

            if (resultSet.next()) {
                // 处理查询结果
                double maxWap = resultSet.getDouble("max_wap");
                double minWap = resultSet.getDouble("min_wap");
                return new double[]{maxWap, minWap};
            }
        } catch (SQLException e) {
            logger.error("get wap failed:" + e.getMessage());
        }
        return null;
    }


}