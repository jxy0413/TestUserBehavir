package cn.bjfu.beans;

import lombok.*;

/**
 * Created by jxy on 2021/4/22 0022 10:08
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class MarketingUserBehavior {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timeStamp;
}
