package capital.daphne;

import capital.daphne.strategy.Sma;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.tablesaw.api.FloatColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;


public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);

    public static void main(String[] args) throws ParseException {
        //testTimeHanleing();
        // testEnum();
    }

    private static void testTimeHanleing() {
        Table df = Table.create("IBKR Bar Dataframe");
        // 添加列到表格
        df.addColumns(
                StringColumn.create("date_us"),
                FloatColumn.create("vwap"),
                FloatColumn.create("volatility"));
        try {
            String date = parseTime("20230925 12:13:55 US/Eastern");
            df.stringColumn("date_us").append(date);
            df.floatColumn("vwap").append((float) 176.4);
            df.floatColumn("volatility").append((float) 0.000134715989826);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        System.out.println(df.toString());
        Row row = df.row(df.rowCount() - 1);
        String date_us = row.getString("date_us");
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(date_us, formatter);
        LocalDateTime datetime = zonedDateTime.toLocalDateTime();
        LocalTime time = datetime.toLocalTime();
        System.out.println("datetime=" + datetime.toString());
        System.out.println("time=" + time.toString());

        LocalTime portfolioCloseTime = LocalTime.of(15, 50, 0);
        System.out.println("portfolioCloseTime=" + portfolioCloseTime.toString());
    }

    private static String parseTime(String inputDateTime) throws ParseException {
        // inputDateTime = "20230925 12:13:55 US/Eastern";

        // 定义输入日期时间的格式
        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyyMMdd HH:mm:ss VV");

        // 解析输入日期时间字符串
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(inputDateTime, inputFormatter);

        // 定义输出日期时间的格式
        DateTimeFormatter outputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ssXXX");

        // 格式化日期时间，并包括美国东部时区的偏移量
        String outputDateTime = zonedDateTime.format(outputFormatter);
        return outputDateTime;
    }

    private static void testEnum() {
        System.out.println(Sma.TradeActionType.NO_ACTION);
        System.out.println(Sma.TradeActionType.BUY);
        System.out.println(Sma.TradeActionType.SELL);

    }

    private static void testSqlite() {
        Connection connection = null;

        try {
            // 连接到 SQLite 数据库
            connection = DriverManager.getConnection("jdbc:sqlite:/path/to/your/database.db");

            // 创建表格
            String createTableSQL = "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, age INTEGER)";
            connection.createStatement().executeUpdate(createTableSQL);

            // 插入数据
            String insertSQL = "INSERT INTO users (name, age) VALUES (?, ?)";
            PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
            preparedStatement.setString(1, "John");
            preparedStatement.setInt(2, 30);
            preparedStatement.executeUpdate();

            preparedStatement.setString(1, "Alice");
            preparedStatement.setInt(2, 25);
            preparedStatement.executeUpdate();

            System.out.println("Data inserted successfully.");

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭数据库连接
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}