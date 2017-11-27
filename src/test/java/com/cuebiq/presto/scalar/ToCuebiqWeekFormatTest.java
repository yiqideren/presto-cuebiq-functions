package com.cuebiq.presto.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

import java.text.ParseException;

public class ToCuebiqWeekFormatTest {

    @Test
    public void testLastWeekOfYear() throws ParseException {
        Slice week = ToCuebiqWeekFormat.to_cuebiq_week_format(Slices.utf8Slice("20160101"), Slices.utf8Slice("yyyyMMdd"));
        Assert.assertEquals("2015-W53-1", week.toStringUtf8());
    }

    @Test
    public void testFirstWeekOfYear() throws ParseException {
        Slice week = ToCuebiqWeekFormat.to_cuebiq_week_format(Slices.utf8Slice("20160104"), Slices.utf8Slice("yyyyMMdd"));
        Assert.assertEquals("2016-W01-1", week.toStringUtf8());
    }

    @Test
    public void testDateFormat() throws ParseException {
        Slice week = ToCuebiqWeekFormat.to_cuebiq_week_format(Slices.utf8Slice("20160104"), Slices.utf8Slice("%Y%m%d"));
        Assert.assertEquals("2016-W01-1", week.toStringUtf8());
    }
}
