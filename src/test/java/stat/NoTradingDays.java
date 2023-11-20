package stat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class NoTradingDays {
    public static void main(String[] args) {
        String currentDirectory = System.getProperty("user.dir");
        String csvFilePath = currentDirectory + "/src/test/java/stat/file_5b471549-941e-4a29-8e91-1294ae86b4ca.csv"; // 替换为实际的 CSV 文件路径
        List<LocalDate> tradingDates = readAndParseCSV(csvFilePath);
        int longestNoTradingDays = findLongestNoTradingDays(tradingDates);
        System.out.println("Longest number of consecutive no trading days: " + longestNoTradingDays);
    }

    private static List<LocalDate> readAndParseCSV(String filePath) {
        List<LocalDate> tradingDates = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                if (values.length >= 5) { // Assuming date is in the fifth column
                    String dateString = values[4].trim();
                    if (dateString.equals("date")) {
                        continue;
                    }
                    LocalDate date = LocalDate.parse(dateString, DateTimeFormatter.ISO_DATE_TIME);
                    tradingDates.add(date);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        Collections.sort(tradingDates); // Sort the trading dates
        return tradingDates;
    }

    private static int findLongestNoTradingDays(List<LocalDate> tradingDates) {
        int longestNoTradingDays = 0;
        int currentNoTradingDays = 0;

        for (int i = 1; i < tradingDates.size(); i++) {
            LocalDate currentDate = tradingDates.get(i);
            LocalDate previousDate = tradingDates.get(i - 1);
            // Check if there is a gap between consecutive dates
            if (!currentDate.minusDays(1).equals(previousDate)) {
                currentNoTradingDays = 0; // Reset the counter
            } else {
                System.out.println(previousDate + "|" + currentDate);
                currentNoTradingDays++;
                longestNoTradingDays = Math.max(longestNoTradingDays, currentNoTradingDays);
            }
        }

        return longestNoTradingDays;
    }
}
