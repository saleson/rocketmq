package org.apache.rocketmq.client.ext.producer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author saleson
 * @date 2022-05-18 22:48
 */
public class ExtProducerHelper {

    private static List<MessageTransformer> staticMessageTransformers = Collections.emptyList();

    public static MessageTransformer multiSPIMessageTransformer() {
        ServiceLoader<MessageTransformer> spi = ServiceLoader.load(MessageTransformer.class);
        List<MessageTransformer> list = new ArrayList<>(staticMessageTransformers);
        spi.iterator().forEachRemaining(list::add);
        return new MultiMessageTransformer(list);
    }


    public static synchronized void addStaticMessageTransformer(MessageTransformer messageTransformer){
        List<MessageTransformer> staticMessageTransformers = new ArrayList<>(ExtProducerHelper.staticMessageTransformers);
        staticMessageTransformers.add(messageTransformer);
        ExtProducerHelper.staticMessageTransformers = staticMessageTransformers;
    }




}
