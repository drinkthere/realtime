package capital.daphne.utils;

import java.util.Calendar;
import java.util.TimeZone;

public class Utils {
    public static boolean isMarketOpen() {
        Calendar calendar = Calendar.getInstance();

        // 设置美东时区
        TimeZone timeZone = TimeZone.getTimeZone("America/New_York"); // 美东时区
        calendar.setTimeZone(timeZone);

        // 获取当前日期和时间信息
        int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
        int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);
        int minute = calendar.get(Calendar.MINUTE);

        // 判断是否在周一到周五的早上9:30到下午4:00之间
        if (dayOfWeek >= Calendar.MONDAY && dayOfWeek <= Calendar.FRIDAY) {
            if (hourOfDay == 9 && minute >= 30) {
                return true;
            } else if (hourOfDay > 9 && hourOfDay < 16) {
                return true;
            } else if (hourOfDay == 16 && minute == 0) {
                return true;
            }
        }

        return false;
    }
}
