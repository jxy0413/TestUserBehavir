package cn.bjfu.beans;

import lombok.*;

/**
 * Created by jxy on 2021/4/22 0022 10:10
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class ChannelPromotionCount {
    private String channel;
    private String behavior;
    private String windowEnd;
    private Long count;
}
