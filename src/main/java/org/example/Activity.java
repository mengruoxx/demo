package org.example;

import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.date.DateUtil;
import lombok.Data;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * @author mengruo
 */
public class Activity {

    @Data
    static class UserScore {
        private Integer userId;
        /**
         * 排名
         */
        private Long rank;
        /**
         * 分数
         */
        private Long score;
    }

    @Autowired
    private RedisTemplate<String, Integer> redisTemplate;

    private final static String SCORE_KEY = "activity_score_zset:%s";

    private String getKey() {
        return String.format(SCORE_KEY, DateUtil.format(DateUtil.date(), "yyyyMM"));
    }

    public List<UserScore> getRankList(int userId) {
        // 获取当前用户排名
        Long rank = redisTemplate.opsForZSet().reverseRank(getKey(), userId);
        // 还没有排名，按实际需求处理
        if (Objects.isNull(rank)) {
            return Collections.emptyList();
        }
        // 获取前后10名用户的分数排名
        Set<ZSetOperations.TypedTuple<Integer>> userScores = redisTemplate.opsForZSet().reverseRangeWithScores(getKey(),
                Math.max(0, rank - 10), rank + 10);
        if (CollectionUtil.isEmpty(userScores)) {
            return Collections.emptyList();
        }
        // 当前用户的排名
        long userRank = rank + 1;
        // 查询出的数据的起始排名
        long currentRank = userRank > 10L ? userRank - 10L : 1;
        return buildUserScores(userScores, currentRank);
    }

    private List<UserScore> buildUserScores(Set<ZSetOperations.TypedTuple<Integer>> userScores, long currentRank) {
        List<UserScore> results = new ArrayList<>();
        for (ZSetOperations.TypedTuple<Integer> tuple : userScores) {
            if (Objects.nonNull(tuple.getScore()) && Objects.nonNull(tuple.getValue())) {
                UserScore userScore = new UserScore();
                userScore.setUserId(tuple.getValue());
                userScore.setScore(getUserScore(tuple.getScore().longValue()));
                userScore.setRank(currentRank);
                currentRank++;
                results.add(userScore);
            }
        }
        return results;
    }

    /**
     * 给用户增加分数并保存数据库
     * @param userId
     * @param delta
     */
    public long incrScores(int userId, int delta) {
        Double score = redisTemplate.opsForZSet().score(getKey(), userId);
        if (Objects.isNull(score)) {
            score = 0.0;
        }
        long userScore = getUserScore(score.longValue());
        redisTemplate.opsForZSet().add(getKey(), userId, getSaveScore(userScore + delta));
        return userScore + delta;
    }

    /**
     * 获取前n个排名榜单
     * @param n
     * @return
     */
    public List<UserScore> getTopNRankList(int n) {
        if (n <= 0) {
            return Collections.emptyList();
        }
        // 获取前后10名用户的分数排名
        Set<ZSetOperations.TypedTuple<Integer>> userScores = redisTemplate.opsForZSet().reverseRangeWithScores(getKey(), 0, n - 1);
        if (CollectionUtil.isEmpty(userScores)) {
            return Collections.emptyList();
        }
        return buildUserScores(userScores, 1);
    }

    /**
     * 根据zset的score算出用户分数
     * @param saveScore
     * @return
     */
    private long getUserScore(long saveScore) {
        return saveScore / 10000000000L;
    }

    /**
     * 用用户得分计算存入数据库的score，
     * 得分 拼接 （未来时间戳 - 现在时间戳）
     * @param userScore
     * @return
     */
    private long getSaveScore(long userScore) {
        return userScore * 10000000000L + (DateUtil.endOfMonth(DateUtil.date()).toTimestamp().getTime() - System.currentTimeMillis());
    }

}