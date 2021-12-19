package org.apache.flink.ceppro;


import org.apache.flink.ceppro.pattern.Pattern;

import java.io.Serializable;
import java.util.Map;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/19 下午8:51
 */
public abstract class PatternChangeListener<T> implements Serializable {
    private Boolean isChange = false;
    private ChangeTypeEnum changeType;

    abstract public void open();
    abstract public void close();
    public boolean needChange(T element) {
        return isChange;
    }
    abstract public Map<String,String> getNewPatternString(T element);

    public Map<String,Pattern<T,?>> getNewPattern(T element) {
        return null;
    }

    public static enum ChangeTypeEnum {
        TIMERAGE,
        EVENT
    }
}
