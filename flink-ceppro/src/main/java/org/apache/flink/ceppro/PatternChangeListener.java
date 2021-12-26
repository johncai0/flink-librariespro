package org.apache.flink.ceppro;


import org.apache.commons.lang3.StringUtils;
import org.apache.flink.ceppro.pattern.Pattern;
import org.apache.flink.ceppro.util.CustomStringJavaCompiler;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;

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
     * 例如：这个方法可以从redis中拉取出来一个value，如果这个值等于类中存在的值，则不需要更新规则，
     * 所以，称这个值为verion，版本相同不更新，版本不同则需要更新。
     * @return
     */
    abstract public String patternVersion();

    /**
     * 获取新的pattern字符串
     * 从redis或者外部存储中，获取新的pattern源码
     * @return
     */
    abstract public String getNewPatternString();

    /**
     * 获取需要删除的pattern的set<key>
     * @return
     */
    abstract public Set<String> getDelPatternKey();

    /**
     * 如果需要初始化patternStream的时候从外部存储初始化pattern，则需要覆盖这个方法。
     * 这个方法执行再open前边，因此需要注意资源的初始化问题。
     * @return
     */
    public String getInitPatternString() {
        return null;
    }

    /**
     * 获取一个更新规则的时间间隔，默认1分钟
     * 返回时长的毫秒数
     * @return
     */
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

    public Map<String,Pattern<T,?>> getInitPattern() {
        try {
            String code = this.getInitPatternString();
            if (StringUtils.isBlank(code)) return null;
            CustomStringJavaCompiler<Pattern<T, ?>> compiler = new CustomStringJavaCompiler(code);
            return compiler.runCustomMethod();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
