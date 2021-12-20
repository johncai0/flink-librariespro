package org.apache.flink.ceppro;


import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.ceppro.util.CustomStringJavaCompiler;

import java.io.Serializable;
import java.util.Map;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/19 下午8:51
 */
public abstract class PatternChangeListener<T> implements Serializable {

    /**
     * 打开获取pattern字符串的连接等等
     */
    abstract public void open();
    /**
     * 关闭获取pattern字符串的连接等等
     */
    abstract public void close();

    /**
     * 判断pattern是否需要变更
     * @return
     */
    abstract public boolean needChange();

    /**
     * 获取新的pattern字符串
     * @return
     */
    abstract public String getNewPatternString();

    public long getUpdateInterval() {
        return 60*1000;
    }

    public Map<String,Pattern<T,?>> getNewPattern() {
        try {
            String code = this.getNewPatternString();
            CustomStringJavaCompiler<Pattern<T, ?>> compiler = new CustomStringJavaCompiler(code);
            return compiler.runCustomMethod();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
