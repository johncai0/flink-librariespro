package org.apache.flink.ceppro;

import org.apache.flink.ceppro.pattern.Pattern;

import java.io.Serializable;
import java.util.Map;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/19 下午9:44
 */
public interface GeneratePatternInterface<T> extends Serializable {
    Map<String,Pattern<T,?>> getPatternMap();
}
