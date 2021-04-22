package cn.bjfu.beans;

import lombok.*;

/**
 * Created by jxy on 2021/4/22 0022 11:10
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class AdCountViewByProvince {
    private String province;
    private String windowEnd;
    private Long count;
}
