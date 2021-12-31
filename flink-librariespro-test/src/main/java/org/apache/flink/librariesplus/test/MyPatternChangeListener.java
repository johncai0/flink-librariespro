package org.apache.flink.librariesplus.test;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.ceppro.PatternChangeListener;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    private static final Logger LOG = LoggerFactory.getLogger(MyPatternChangeListener.class);
    Jedis jedis = null;

    @Override
    public void open(Configuration parameters) {
        LOG.info("open Pattern Change Lister funcation.");
        jedis = new Jedis("hadoop01");
    }

    @Override
    public void close() {
        if (jedis != null && jedis.isConnected()) jedis.close();
        LOG.info("close Pattern Change Lister funcation.");
    }

    @Override
    public String patternVersion() {
        LOG.info("in Pattern Change Lister funcation get pattern version.");
        return jedis.get("ptv");
    }

    @Override
    public long getUpdateInterval() {
        return 5000L;
    }

    @Override
    public String getNewPatternString() {
        LOG.info("in Pattern Change Lister funcation getNewPatternString.");
        String version = patternVersion();
        String patternStr = jedis.get("pattern_"+version);
        LOG.info("in Pattern Change Lister funcation getNewPatternString. pattern: \n{}",patternStr);
        if (StringUtils.isNotBlank(patternStr)) {
            return patternStr;
        }
        return null;
    }

    @Override
    public Set<String> getDelPatternKey() {
        LOG.info("in Pattern Change Lister funcation getDelPatternKey.");
        String delKeys = jedis.get("del_pattern_key");
        Set<String> delSet = Arrays.stream(delKeys.split(",")).collect(Collectors.toSet());
        LOG.info("in Pattern Change Lister funcation getDelPatternKey. delList: {}",delSet);
        if (CollectionUtils.isNotEmpty(delSet)) return delSet;
        return null;
    }
}
