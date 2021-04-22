package cn.bjfu.beans;

import lombok.*;

/**
 * Created by jxy on 2021/4/22 0022 17:17
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@ToString
@Data
public class BlackListUserWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;
}
