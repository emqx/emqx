import java.io.*;
import java.util.*;
import com.erlport.erlang.term.*;
import com.erlport.*;

class State implements Serializable {

    Integer times;

    public State() {
        times = 0;
    }

    public Integer incr() {
        times += 1;
        return times;
    }

    @Override
    public String toString() {
        return String.format("State(times: %d)", times);
    }
}

public class Main {

    static Integer OK = 0;
    static Integer ERROR = 0;

    //-------------------
    // Connection level

    public static Object init(Object conn, Object connInfo) {
        System.err.printf("[java] established a conn=%s, connInfo=%s\n", conn, connInfo);

        // set an instance to be the connection state
        // it just a example structure to record the callback total times
         Object state = new State();

        // subscribe the topic `t/dn` with qos0
        subscribe(conn, new Binary("t/dn"), 0);

        // return the initial conn's state
        return Tuple.two(OK, state);
    }

    public static Object received(Object conn, Object data, Object state) {
        System.err.printf("[java] received data conn=%s, data=%s, state=%s\n", conn, data, state);

        // echo the conn's data
        send(conn, data);

        // return the new conn's state
        State nstate = (State) state;
        nstate.incr();
        return Tuple.two(OK, nstate);
    }

    public static void terminated(Object conn, Object reason,  Object state) {
        System.err.printf("[java] terminated conn=%s, reason=%s, state=%s\n", conn, reason, state);
        return;
    }

    //-----------------------
    // Protocol/Session level
    
    public static Object deliver(Object conn, Object msgs0,  Object state) {
        System.err.printf("[java] received messages conn=%s, msgs=%s, state=%s\n", conn, msgs0, state);

        List<Object> msgs = (List<Object>) msgs0;
        for(Object msg: msgs) {
            publish(conn, msg);
        }

        // return the new conn's state
        State nstate = (State) state;
        nstate.incr();
        return Tuple.two(OK, nstate);
    }

    //-----------------------
    // APIs
    public static void send(Object conn, Object data) {
        try {
            Erlang.call("emqx_exproto", "send", new Object[]{conn, data}, 5000);
        } catch (Exception e) {
            System.err.printf("[java] send data error: %s\n", e);
        }
        return;
    }

    public static void close(Object conn) {
        try {
            Erlang.call("emqx_exproto", "close", new Object[]{conn}, 5000);
        } catch (Exception e) {
            System.err.printf("[java] send data error: %s\n", e);
        }
        return;
    }

    public static void register(Object conn, Object clientInfo) {
        try {
            Erlang.call("emqx_exproto", "register", new Object[]{conn, clientInfo}, 5000);
        } catch (Exception e) {
            System.err.printf("[java] send data error: %s\n", e);
        }
        return;
    }

    public static void publish(Object conn, Object message) {
        try {
            Erlang.call("emqx_exproto", "publish", new Object[]{conn, message}, 5000);
        } catch (Exception e) {
            System.err.printf("[java] send data error: %s\n", e);
        }
        return;
    }

    public static void subscribe(Object conn, Object topic, Object qos) {
        try {
            Erlang.call("emqx_exproto", "subscribe", new Object[]{conn, topic, qos}, 5000);
        } catch (Exception e) {
            System.err.printf("[java] send data error: %s\n", e);
        }
        return;
    }

    public static void unsubscribe(Object conn, Object topic) {
        try {
            Erlang.call("emqx_exproto", "unsubscribe", new Object[]{conn, topic}, 5000);
        } catch (Exception e) {
            System.err.printf("[java] send data error: %s\n", e);
        }
        return;
    }
}
