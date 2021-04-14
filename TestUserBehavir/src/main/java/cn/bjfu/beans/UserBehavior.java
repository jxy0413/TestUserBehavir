package cn.bjfu.beans;

import lombok.*;

/**
 * Created by jxy on 2021/4/14 0014 18:48
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timeStamp;
}
