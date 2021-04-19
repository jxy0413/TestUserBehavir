package cn.bjfu.beans;

import lombok.*;

/**
 * Created by jxy on 2021/4/15 0015 19:21
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class ApacheLogEvent {
    private String ip;
    private String userId;
    private Long timeStamp;
    private String method;
    private String url;
}
