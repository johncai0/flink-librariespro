package org.apache.flink.librariesplus.test;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ceppro.PatternChangeListener;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author johnCai
 * @version 1.0
 * @date 2021/12/27 下午10:48
 */
public class MyPatternChangeListener extends PatternChangeListener<Tuple3<String,Long,String>> {

    Jedis jedis = null;

    @Override
    public void open() {
        jedis = new Jedis("hadoop01");
    }

    @Override
    public void close() {
        if (jedis != null && jedis.isConnected()) jedis.close();
    }

    @Override
    public String patternVersion() {
        return jedis.get("ptv");
    }

    @Override
    public String getNewPatternString() {
        String version = patternVersion();
        String patternStr = jedis.get("pattern_"+version);
        if (StringUtils.isNotBlank(patternStr)) {
            return patternStr;
        }
        return null;
    }

    @Override
    public Set<String> getDelPatternKey() {
        String delKeys = jedis.get("del_pattern_key");
        Set<String> delSet = Arrays.stream(delKeys.split(",")).collect(Collectors.toSet());
        if (CollectionUtils.isNotEmpty(delSet)) return delSet;
        return null;
    }
}
