package com.cuebiq.presto.scalar;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DatesFunctions {

    @ScalarFunction("to_cuebiq_week_format")
    @Description("convert dates in cuebiq week format")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice to_cuebiq_week_format(@SqlType(StandardTypes.VARCHAR) Slice date, @SqlType(StandardTypes.VARCHAR) Slice dateFormat) throws ParseException {

        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat.toStringUtf8());

        Calendar calendar = Calendar.getInstance();
        calendar.setMinimalDaysInFirstWeek(4);
        calendar.setFirstDayOfWeek(Calendar.MONDAY);
        calendar.setTime(formatter.parse(date.toStringUtf8()));

        int week = calendar.get(Calendar.WEEK_OF_YEAR);
        int year = calendar.getWeekYear();

        return Slices.utf8Slice(year + "-W" + (week < 10 ? "0" + week : week) + "-1");
    }

}
