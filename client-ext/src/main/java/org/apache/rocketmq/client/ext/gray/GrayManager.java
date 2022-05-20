package org.apache.rocketmq.client.ext.gray;

import java.util.Objects;

/**
 * @author saleson
 * @date 2022-05-18 20:20
 */
public class GrayManager {

    public static final String MSG_ROUTING_TAG_KEY = "routing_tag";

    public static GrayManager INSTANCE = new GrayManager(true, "gray");

    private boolean onlyAccessGrayMessage = true;
    private String grayTag;

    public GrayManager(boolean onlyAccessGrayMessage, String grayTag) {
        this.onlyAccessGrayMessage = onlyAccessGrayMessage;
        this.grayTag = grayTag;
    }

    public boolean isOnlyAccessGrayMessage(){
        return onlyAccessGrayMessage;
    }

    public String getGrayTag(){
        return grayTag;
    }

}
