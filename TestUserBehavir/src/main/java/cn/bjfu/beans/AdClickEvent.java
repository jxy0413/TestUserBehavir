package cn.bjfu.beans;

import lombok.*;

/**
 * Created by jxy on 2021/4/22 0022 11:04
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class AdClickEvent {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;
}
