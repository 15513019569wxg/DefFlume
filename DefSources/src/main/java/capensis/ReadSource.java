package capensis;/*
    @author wxg
    @date 2021/6/22-8:37
    */


import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ReadSource extends AbstractSource implements Configurable, PollableSource {
    //  定义配置文件将来要被读取的字段
    private Long delay;
    private String field;

    /**
     * 读取配置文件
     * @param context   上下文连接器
     */
    @Override
    public void configure(Context context) {
        delay = context.getLong("delay");
        field = context.getString("field", "hello");
    }

    /**
     * 处理数据
     * @return  事件状态
     * @throws EventDeliveryException   事件提交失败
     */
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        try {
            //   创建事件头信息
            HashMap<String, String> headerMap = new HashMap<>();
            //  创建事件
            SimpleEvent simpleEvent = new SimpleEvent();
            //  循环封装事件
            for(int i = 0; i < 5; i++){
                //  给事件设置头信息
                simpleEvent.setHeaders(headerMap);
                //  给事件设置内容
                simpleEvent.setBody((field + i + delay).getBytes());
                //  将事件写入channel
                getChannelProcessor().processEvent(simpleEvent);
                Thread.sleep(2000);
            }
            Thread.sleep(8000);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Status backoff = Status.BACKOFF;
            return backoff;
        }
        Status ready = Status.READY;
        return ready;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
