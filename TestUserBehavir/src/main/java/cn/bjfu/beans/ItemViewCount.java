package cn.bjfu.beans;


import lombok.*;

/**
 * Created by jxy on 2021/4/14 0014 18:51
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class ItemViewCount {
    private Long itemId;
    private Long windowEnd;
    private Long count;
}
