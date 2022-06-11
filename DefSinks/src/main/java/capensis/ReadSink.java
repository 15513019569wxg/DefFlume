package capensis;/*
    @author wxg
    @date 2021/6/22-9:57
    */


import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadSink extends AbstractSink implements Configurable {
    //  创建logger对象
    private Logger logger = LoggerFactory.getLogger(ReadSink.class);
    private String prefix;
    private String suffix;


    @Override
    public void configure(Context context) {
        //  读取配置文件内容，有默认值
        prefix = context.getString("prefix", "hello");
        //  读取配置文件内容，无默认值
        suffix = context.getString("suffix");

    }

    @Override
    public Status process() throws EventDeliveryException {
        //  声明返回值状态信息
        Status status;

        //  获取当前Sink绑定的Channel
        Channel channel = getChannel();

        // 获取事务
        Transaction transaction = channel.getTransaction();
        //  开启事务
        transaction.begin();
        try {
            //  获取事件
            Event event = channel.take();
            //  读取Channel中的事件，直到读取到事件结束循环
            if (event != null)
                //  处理事件
                logger.info(prefix + new String(event.getBody()) + suffix);
            //  提交事务
            transaction.commit();
            status = Status.READY;
        } catch (Exception e) {
            e.printStackTrace();
            status = Status.BACKOFF;
        } finally {
            //  关闭事务
            transaction.close();
        }
        return status;

    }

}
