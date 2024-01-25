-module(emqx_bridge_rabbitmq_source_worker).

-behaviour(gen_server).

-export([start_link/1]).
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-include_lib("amqp_client/include/amqp_client.hrl").

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, []).

init({_RabbitChannel, _InstanceId, _Params} = State) ->
    {ok, State, {continue, confirm_ok}}.

handle_continue(confirm_ok, State) ->
    receive
        #'basic.consume_ok'{} -> {noreply, State}
    end.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(
    {#'basic.deliver'{delivery_tag = Tag}, #amqp_msg{
        payload = Payload,
        props = #'P_basic'{message_id = MessageId, headers = Headers}
    }},
    {Channel, InstanceId, Params} = State
) ->
    #{
        hookpoints := Hooks,
        payload_template := PayloadTmpl,
        qos := QoSTmpl,
        topic := TopicTmpl,
        no_ack := NoAck
    } = Params,
    MQTTMsg = emqx_message:make(
        make_message_id(MessageId),
        InstanceId,
        render(Payload, QoSTmpl),
        render(Payload, TopicTmpl),
        render(Payload, PayloadTmpl),
        #{},
        make_headers(Headers)
    ),
    _ = emqx:publish(MQTTMsg),
    lists:foreach(fun(Hook) -> emqx_hooks:run(Hook, [MQTTMsg]) end, Hooks),
    (NoAck =:= false) andalso
        amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    emqx_resource_metrics:received_inc(InstanceId),
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {stop, normal, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

render(_Message, QoS) when is_integer(QoS) -> QoS;
render(Message, PayloadTmpl) ->
    Opts = #{return => full_binary},
    emqx_placeholder:proc_tmpl(PayloadTmpl, Message, Opts).

make_message_id(undefined) -> emqx_guid:gen();
make_message_id(Id) -> Id.

make_headers(undefined) ->
    #{};
make_headers(Headers) when is_list(Headers) ->
    maps:from_list([{Key, Value} || {Key, _Type, Value} <- Headers]).
