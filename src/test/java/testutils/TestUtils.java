package testutils;

import capital.daphne.models.BarInfo;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import testmodels.Bar;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {
    public static List<Bar> loadCsv(String csvFilePath) {
        List<Bar> bars = new ArrayList<>();

        try (FileReader fileReader = new FileReader(csvFilePath);
             CSVParser csvParser = new CSVParser(fileReader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {

            for (CSVRecord csvRecord : csvParser) {
                Bar bar = new Bar();
                bar.setDate(csvRecord.get("date"));
                bar.setOpen(Double.parseDouble(csvRecord.get("open")));
                bar.setHigh(Double.parseDouble(csvRecord.get("high")));
                bar.setLow(Double.parseDouble(csvRecord.get("low")));
                bar.setClose(Double.parseDouble(csvRecord.get("close")));
                bar.setVolume(Double.parseDouble(csvRecord.get("volume")));
                bar.setVwap(Double.parseDouble(csvRecord.get("average")));
                bar.setBarCount(Integer.parseInt(csvRecord.get("barCount")));
                bars.add(bar);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return bars;
    }

    public static Table getTable(List<BarInfo> barList, int minBarNum) {
        Table dataframe = Table.create("IBKR Bar Dataframe");
        // 添加列到表格
        dataframe.addColumns(
                StringColumn.create("date_us"),
                DoubleColumn.create("vwap"),
                DoubleColumn.create("open"),
                DoubleColumn.create("close"),
                DoubleColumn.create("high"),
                DoubleColumn.create("low"),
                DoubleColumn.create("volatility"));

        List<BarInfo> validBarList = barList.subList(barList.size() - minBarNum, barList.size());
        for (BarInfo bar : validBarList) {
            dataframe.stringColumn("date_us").append(bar.getDate());
            dataframe.doubleColumn("vwap").append(bar.getVwap());
            dataframe.doubleColumn("open").append(bar.getOpen());
            dataframe.doubleColumn("high").append(bar.getHigh());
            dataframe.doubleColumn("low").append(bar.getLow());
            dataframe.doubleColumn("close").append(bar.getClose());
            dataframe.doubleColumn("volatility").append(bar.getVolatility());
        }
        DoubleColumn prevVWapColumn = dataframe.doubleColumn("vwap").lag(1);
        dataframe.addColumns(prevVWapColumn.setName("prev_vwap"));
        return dataframe;
    }
}
