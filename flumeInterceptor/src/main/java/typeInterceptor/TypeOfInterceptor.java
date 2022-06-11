package typeInterceptor;/*
    @author wxg
    @date 2021/6/21-11:32
    */


import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TypeOfInterceptor implements Interceptor {
    //声明一个事件集合
    private List<Event> addHeaderEvents;


    @Override
    public void initialize() {
        addHeaderEvents = new ArrayList<>();

    }
    //单个事件拦截
    @Override
    public Event intercept(Event event) {
        //  1、获取事件的头信息
        Map<String, String> headers = event.getHeaders();
        //  2、获取事件的body信息
        String body = new String(event.getBody());
        //  3、根据body中是否有“hello”来决定添加怎样的头信息
        if(body.contains("hello")) {
            //  4、添加头信息
            headers.put("type", "wxg");
        }else{
            headers.put("type", "other");
        }
        return event;
    }
    //批量事件拦截
    @Override
    public List<Event> intercept(List<Event> events) {
        //  1、清空集合
        addHeaderEvents.clear();
        //  2、给每一个事件添加头信息
        events.forEach(event -> addHeaderEvents.add(intercept(event)));
        return addHeaderEvents;
    }
    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder{

        @Override
        public Interceptor build() {
            return new TypeOfInterceptor();
        }

        @Override
        public void configure(Context context) {

        }
    }
}


