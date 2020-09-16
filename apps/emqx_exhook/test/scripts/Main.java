import java.io.*;
import java.util.*;
import com.erlport.erlang.term.*;

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

    public static Object init() {
        System.err.printf("Initiate driver...\n");

        // [{"topics", ["t/#", "t/a"]}]
        List<Object> topics = new ArrayList<Object>();
        topics.add(new Binary("t/#"));
        topics.add(new Binary("test/#"));

        List<Object> actionOpts  = new ArrayList<Object>();
        actionOpts.add(Tuple.two(new Atom("topics"), topics));

        Object[] actions0 = new Object[] {
            Tuple.three("client_connect", "Main", "on_client_connect"),
            Tuple.three("client_connack", "Main", "on_client_connack"),
            Tuple.three("client_connected", "Main", "on_client_connected"),
            Tuple.three("client_disconnected", "Main", "on_client_disconnected"),
            Tuple.three("client_authenticate", "Main", "on_client_authenticate"),
            Tuple.three("client_check_acl", "Main", "on_client_check_acl"),
            Tuple.three("client_subscribe", "Main", "on_client_subscribe"),
            Tuple.three("client_unsubscribe", "Main", "on_client_unsubscribe"),

            Tuple.three("session_created", "Main", "on_session_created"),
            Tuple.three("session_subscribed", "Main", "on_session_subscribed"),
            Tuple.three("session_unsubscribed", "Main", "on_session_unsubscribed"),
            Tuple.three("session_resumed", "Main", "on_session_resumed"),
            Tuple.three("session_discarded", "Main", "on_session_discarded"),
            Tuple.three("session_takeovered", "Main", "on_session_takeovered"),
            Tuple.three("session_terminated", "Main", "on_session_terminated"),

            Tuple.four("message_publish", "Main", "on_message_publish", actionOpts),
            Tuple.four("message_delivered", "Main", "on_message_delivered", actionOpts),
            Tuple.four("message_acked", "Main", "on_message_acked", actionOpts),
            Tuple.four("message_dropped", "Main", "on_message_dropped", actionOpts)
        };

        List<Object> actions = new ArrayList<Object>(Arrays.asList(actions0));

        State state = new State();
        //Tuple state = new Tuple(0);

        // {0 | 1, [{HookName, CallModule, CallFunction, Opts}]}
        return Tuple.two(0, Tuple.two(actions, state));
    }

    public static void deinit() {

    }

    // Callbacks

    public static void on_client_connect(Object connInfo, Object props, Object state) {
        System.err.printf("[Java] on_client_connect: connInfo: %s, props: %s, state: %s\n", connInfo, props, state);
    }

    public static void on_client_connack(Object connInfo, Object rc, Object props, Object state) {
        System.err.printf("[Java] on_client_connack: connInfo: %s, rc: %s, props: %s, state: %s\n", connInfo, rc, props, state);
    }

    public static void on_client_connected(Object clientInfo, Object state) {
        System.err.printf("[Java] on_client_connected: clientinfo: %s, state: %s\n", clientInfo, state);
    }

    public static void on_client_disconnected(Object clientInfo, Object reason, Object state) {
        System.err.printf("[Java] on_client_disconnected: clientinfo: %s, reason: %s, state: %s\n", clientInfo, reason, state);
    }

    public static Object on_client_authenticate(Object clientInfo, Object authresult, Object state) {
        System.err.printf("[Java] on_client_authenticate: clientinfo: %s, authresult: %s, state: %s\n", clientInfo, authresult, state);

        return Tuple.two(0, true);
    }

    public static Object on_client_check_acl(Object clientInfo, Object pubsub, Object topic, Object result, Object state) {
        System.err.printf("[Java] on_client_check_acl: clientinfo: %s, pubsub: %s, topic: %s, result: %s, state: %s\n", clientInfo, pubsub, topic, result, state);

        return Tuple.two(0, true);
    }

    public static void on_client_subscribe(Object clientInfo, Object props, Object topic, Object state) {
        System.err.printf("[Java] on_client_subscribe: clientinfo: %s, props: %s, topic: %s, state: %s\n", clientInfo, props, topic, state);
    }

    public static void on_client_unsubscribe(Object clientInfo, Object props, Object topic, Object state) {
        System.err.printf("[Java] on_client_unsubscribe: clientinfo: %s, props: %s, topic: %s, state: %s\n", clientInfo, props, topic, state);
    }

    // Sessions

    public static void on_session_created(Object clientInfo, Object state) {
        System.err.printf("[Java] on_session_created: clientinfo: %s, state: %s\n", clientInfo, state);
    }

    public static void on_session_subscribed(Object clientInfo, Object topic, Object opts, Object state) {
        System.err.printf("[Java] on_session_subscribed: clientinfo: %s, topic: %s, subopts: %s, state: %s\n", clientInfo, topic, opts, state);
    }

    public static void on_session_unsubscribed(Object clientInfo, Object topic, Object state) {
        System.err.printf("[Java] on_session_unsubscribed: clientinfo: %s, topic: %s, state: %s\n", clientInfo, topic, state);
    }

    public static void on_session_resumed(Object clientInfo, Object state) {
        System.err.printf("[Java] on_session_resumed: clientinfo: %s, state: %s\n", clientInfo, state);
    }

    public static void on_session_discarded(Object clientInfo, Object state) {
        System.err.printf("[Java] on_session_discarded: clientinfo: %s, state: %s\n", clientInfo, state);
    }

    public static void on_session_takeovered(Object clientInfo, Object state) {
        System.err.printf("[Java] on_session_takeovered: clientinfo: %s, state: %s\n", clientInfo, state);
    }

    public static void on_session_terminated(Object clientInfo, Object reason, Object state) {
        System.err.printf("[Java] on_session_terminated: clientinfo: %s, reason: %s, state: %s\n", clientInfo, reason, state);
    }

    // Messages

    public static Object on_message_publish(Object message, Object state) {
        System.err.printf("[Java] on_message_publish: message: %s, state: %s\n", message, state);
        return Tuple.two(0, message);
    }

    public static void on_message_dropped(Object message, Object reason, Object state) {
        System.err.printf("[Java] on_message_dropped: message: %s, reason: %s, state: %s\n", message, reason, state);
    }

    public static void on_message_delivered(Object clientInfo, Object message, Object state) {
        System.err.printf("[Java] on_message_delivered: clientinfo: %s, message: %s, state: %s\n", clientInfo, message, state);
    }

    public static void on_message_acked(Object clientInfo, Object message, Object state) {
        System.err.printf("[Java] on_message_acked: clientinfo: %s, message: %s, state: %s\n", clientInfo, message, state);
    }
}
