package com.llf.kafka.batch.util;

import java.util.List;

public interface IConverter {
    public List<String> byte2ListExcludeNonDay(byte[] msg);
}
