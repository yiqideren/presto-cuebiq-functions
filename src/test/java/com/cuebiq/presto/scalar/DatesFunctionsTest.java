package com.cuebiq.presto.scalar;

import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

public class DatesFunctionsTest {

    @Test
    public void testLastWeekOfYear() throws ParseException {
        String week = DatesFunctions.to_cuebiq_week_format(Slices.utf8Slice("20160101"), Slices.utf8Slice("yyyyMMdd")).toStringUtf8();
        Assert.assertEquals(week, "2015-W53-1");
    }

    @Test
    public void testFirstWeekOfYear() throws ParseException {
        String week = DatesFunctions.to_cuebiq_week_format(Slices.utf8Slice("20160104"), Slices.utf8Slice("yyyyMMdd")).toStringUtf8();
        Assert.assertEquals(week, "2016-W01-1");
    }
}
