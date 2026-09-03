%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_exhook_handler).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").

-export([
    on_client_connect/2,
    on_client_connack/3,
    on_client_connected/2,
    on_client_disconnected/3,
    on_client_authenticate/2,
    on_client_authorize/4,
    on_client_subscribe/3,
    on_client_unsubscribe/3
]).

%% Session Lifecircle Hooks
-export([
    on_session_created/2,
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_session_resumed/2,
    on_session_discarded/2,
    on_session_takenover/2,
    on_session_terminated/3
]).

-export([
    on_message_ingress/2,
    on_message_publish/1,
    on_message_dropped/3,
    on_message_delivered/2,
    on_message_acked/2
]).

%% Utils
-export([
    message/1,
    headers/1,
    stringfy/1,
    merge_responsed_bool/2,
    merge_responsed_message/2,
    merge_responsed_topicfilters/2,
    assign_to_message/2,
    clientinfo/1,
    request_meta/0
]).

-elvis([{elvis_style, god_modules, disable}]).

%%--------------------------------------------------------------------
%% Clients
%%--------------------------------------------------------------------

on_client_connect(ConnInfo, Props) ->
    {UserProps, SystemProps} = format_props(Props),
    Req = #{
        conninfo => conninfo(ConnInfo),
        user_props => UserProps,
        props => SystemProps
    },
    emqx_exhook:cast('client.connect', Req).

on_client_connack(ConnInfo, Rc, Props) ->
    {UserProps, SystemProps} = format_props(Props),
    Req = #{
        conninfo => conninfo(ConnInfo),
        result_code => stringfy(Rc),
        user_props => UserProps,
        props => SystemProps
    },
    emqx_exhook:cast('client.connack', Req).

on_client_connected(ClientInfo, _ConnInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    emqx_exhook:cast('client.connected', Req).

on_client_disconnected(ClientInfo, Reason, _ConnInfo) ->
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        reason => stringfy(Reason)
    },
    emqx_exhook:cast('client.disconnected', Req).

on_client_authenticate(ClientInfo, AuthResult) ->
    %% XXX: Bool is missing more information about the atom of the result
    %%      So, the `Req` has missed detailed info too.
    %%
    %%      The return value of `call_fold` just a bool, that has missed
    %%      detailed info too.
    %%
    Bool = AuthResult == ok,
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        result => Bool
    },

    case
        emqx_exhook:call_fold(
            'client.authenticate',
            Req,
            fun merge_responsed_bool/2
        )
    of
        {StopOrOk, #{result := Result0}} when is_boolean(Result0) ->
            Result =
                case Result0 of
                    true -> ok;
                    _ -> {error, not_authorized}
                end,
            {StopOrOk, Result};
        ignore ->
            ignore
    end.

on_client_authorize(AuthzContext, Action, Topic, Result) ->
    Bool = maps:get(result, Result, deny) == allow,
    %% TODO: Support full action in major release
    Type =
        case Action of
            ?authz_action(publish) -> 'PUBLISH';
            ?authz_action(subscribe) -> 'SUBSCRIBE'
        end,
    Req = #{
        %% Keep the protobuf ClientInfo type for compatibility. This value comes from
        %% AuthzContext and does not contain the password in hardened mode.
        clientinfo => clientinfo(AuthzContext),
        type => Type,
        topic => emqx_topic:get_shared_real_topic(Topic),
        result => Bool
    },
    case
        emqx_exhook:call_fold(
            'client.authorize',
            Req,
            fun merge_responsed_bool/2
        )
    of
        {StopOrOk, #{result := Result0}} when is_boolean(Result0) ->
            NResult =
                case Result0 of
                    true -> allow;
                    _ -> deny
                end,
            {StopOrOk, #{result => NResult, from => exhook}};
        ignore ->
            ignore
    end.

on_client_subscribe(ClientInfo, Props, TopicFilters) ->
    {UserProps, SystemProps} = format_props(Props),
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        user_props => UserProps,
        props => SystemProps,
        topic_filters => topicfilters(TopicFilters)
    },
    case emqx_exhook:call_fold('client.subscribe', Req, fun merge_responsed_topicfilters/2) of
        {StopOrOk, #{topic_filters := RespTfs}} ->
            case rewrite_topicfilters(ClientInfo, TopicFilters, RespTfs) of
                {ok, TopicFilters} ->
                    %% Nothing was rewritten, so the accumulator is left alone
                    %% and the rest of the `client.subscribe' chain runs -- but
                    %% only when the server said to continue. Returning the
                    %% filters unchanged is how a server says "I have looked at
                    %% this and no later hook should touch it", and `ignore'
                    %% would downgrade that to "continue", because `emqx_hooks'
                    %% treats any term other than `stop' or `{stop, Acc}' as
                    %% continue. `{stop, TopicFilters}' keeps both properties.
                    unchanged_topicfilters(StopOrOk, TopicFilters);
                {ok, NTopicFilters} ->
                    {StopOrOk, NTopicFilters};
                {error, _Reason} ->
                    ignore
            end;
        ignore ->
            ignore
    end.

unchanged_topicfilters(stop, TopicFilters) ->
    {stop, TopicFilters};
unchanged_topicfilters(_Ok, _TopicFilters) ->
    ignore.

on_client_unsubscribe(ClientInfo, Props, TopicFilters) ->
    {UserProps, SystemProps} = format_props(Props),
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        user_props => UserProps,
        props => SystemProps,
        topic_filters => topicfilters(TopicFilters)
    },
    emqx_exhook:cast('client.unsubscribe', Req).

%%--------------------------------------------------------------------
%% Session
%%--------------------------------------------------------------------

on_session_created(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    emqx_exhook:cast('session.created', Req).

on_session_subscribed(ClientInfo, Topic, SubOpts) ->
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        topic => emqx_topic:maybe_format_share(Topic),
        subopts => subopts(SubOpts)
    },
    emqx_exhook:cast('session.subscribed', Req).

on_session_unsubscribed(ClientInfo, Topic, _SubOpts) ->
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        topic => emqx_topic:maybe_format_share(Topic)
        %% no subopts when unsub
    },
    emqx_exhook:cast('session.unsubscribed', Req).

on_session_resumed(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    emqx_exhook:cast('session.resumed', Req).

on_session_discarded(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    emqx_exhook:cast('session.discarded', Req).

on_session_takenover(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    emqx_exhook:cast('session.takenover', Req).

on_session_terminated(ClientInfo, Reason, _SessInfo) ->
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        reason => stringfy(Reason)
    },
    emqx_exhook:cast('session.terminated', Req).

%%--------------------------------------------------------------------
%% Message
%%--------------------------------------------------------------------

on_message_ingress(_Ctx, #message{topic = <<"$SYS/", _/binary>>}) ->
    ignore;
on_message_ingress(#{authz_ctx := AuthzContext}, Message) ->
    Props = emqx_message:get_header(properties, Message),
    {UserProps, SystemProps} = format_props(Props),
    Req = #{
        clientinfo => clientinfo(AuthzContext),
        message => message(Message),
        user_props => UserProps,
        props => SystemProps
    },
    case
        emqx_exhook:call_fold(
            'message.ingress',
            Req,
            fun merge_responsed_ingress/2
        )
    of
        {stop, #{result := false}} ->
            {stop, {error, not_authorized}};
        {stop, #{message := {error, Reason}}} ->
            {stop, {error, Reason}};
        {StopOrOk, #{message := NMessage}} ->
            {StopOrOk, assign_to_message(NMessage, Message)};
        ignore ->
            ignore
    end.

on_message_publish(#message{topic = <<"$SYS/", _/binary>>}) ->
    ok;
on_message_publish(Message) ->
    Props = emqx_message:get_header(properties, Message),
    {UserProps, SystemProps} = format_props(Props),
    Req = #{
        message => message(Message),
        user_props => UserProps,
        props => SystemProps
    },
    case
        emqx_exhook:call_fold(
            'message.publish',
            Req,
            fun emqx_exhook_handler:merge_responsed_message/2
        )
    of
        {StopOrOk, #{message := NMessage}} ->
            {StopOrOk, assign_to_message(NMessage, Message)};
        ignore ->
            ignore
    end.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason) ->
    ok;
on_message_dropped(Message, _By, Reason) ->
    Req = #{
        message => message(Message),
        reason => stringfy(Reason)
    },
    emqx_exhook:cast('message.dropped', Req).

on_message_delivered(_ClientInfo, #message{topic = <<"$SYS/", _/binary>>}) ->
    ok;
on_message_delivered(ClientInfo, Message) ->
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        message => message(Message)
    },
    emqx_exhook:cast('message.delivered', Req).

on_message_acked(_ClientInfo, #message{topic = <<"$SYS/", _/binary>>}) ->
    ok;
on_message_acked(ClientInfo, Message) ->
    Req = #{
        clientinfo => clientinfo(ClientInfo),
        message => message(Message)
    },
    emqx_exhook:cast('message.acked', Req).

%%--------------------------------------------------------------------
%% Types

format_props(undefined) ->
    {[], []};
format_props(M) when is_map(M) ->
    case maps:take('User-Property', M) of
        error ->
            {[], props(M)};
        {UserProps, SystemProps} ->
            {user_props(UserProps), props(SystemProps)}
    end.

props(M) when is_map(M) ->
    maps:fold(
        fun(K, V, Acc) ->
            [
                #{
                    name => stringfy(K),
                    value => stringfy(V)
                }
                | Acc
            ]
        end,
        [],
        M
    ).

user_props(UserProps) when is_list(UserProps) ->
    lists:map(
        fun({K, V}) ->
            #{name => stringfy(K), value => stringfy(V)}
        end,
        UserProps
    ).

conninfo(
    ConnInfo =
        #{
            clientid := ClientId,
            peername := {PeerHost, PeerPort},
            sockname := {_, SockPort}
        }
) ->
    Username = maps:get(username, ConnInfo, undefined),
    ProtoName = maps:get(proto_name, ConnInfo, undefined),
    ProtoVer = maps:get(proto_ver, ConnInfo, undefined),
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    #{
        node => stringfy(node()),
        clientid => ClientId,
        username => option(Username),
        peerhost => ntoa(PeerHost),
        peerport => PeerPort,
        sockport => SockPort,
        proto_name => ProtoName,
        proto_ver => stringfy(ProtoVer),
        keepalive => Keepalive
    }.

clientinfo(
    ClientInfo =
        #{
            clientid := ClientId,
            username := Username,
            peername := {PeerHost, PeerPort},
            sockport := SockPort,
            protocol := Protocol,
            mountpoint := Mountpoiont
        }
) ->
    #{
        node => stringfy(node()),
        clientid => ClientId,
        username => option(Username),
        password => option(maps:get(password, ClientInfo, undefined)),
        peerhost => ntoa(PeerHost),
        peerport => PeerPort,
        sockport => SockPort,
        protocol => stringfy(Protocol),
        mountpoint => option(Mountpoiont),
        is_superuser => maps:get(is_superuser, ClientInfo, false),
        anonymous => maps:get(anonymous, ClientInfo, true),
        cn => option(maps:get(cn, ClientInfo, undefined)),
        dn => option(maps:get(dn, ClientInfo, undefined))
    }.

message(#message{
    id = Id,
    qos = Qos,
    from = From,
    topic = Topic,
    payload = Payload,
    timestamp = Ts,
    headers = Headers
}) ->
    #{
        node => stringfy(node()),
        id => emqx_guid:to_hexstr(Id),
        qos => Qos,
        from => stringfy(From),
        topic => Topic,
        payload => Payload,
        timestamp => Ts,
        headers => headers(Headers)
    }.

headers(Headers) ->
    Ls = [username, protocol, peerhost, allow_publish],
    maps:fold(
        fun
            (_, undefined, Acc) ->
                %% Ignore undefined value
                Acc;
            (K, V, Acc) ->
                case lists:member(K, Ls) of
                    true ->
                        Acc#{atom_to_binary(K) => bin(K, V)};
                    _ ->
                        Acc
                end
        end,
        #{},
        Headers
    ).

bin(K, V) when
    K == username;
    K == protocol;
    K == allow_publish
->
    bin(V);
bin(peerhost, V) ->
    bin(inet:ntoa(V)).

bin(V) when is_binary(V) -> V;
bin(V) when is_atom(V) -> atom_to_binary(V);
bin(V) when is_list(V) -> iolist_to_binary(V).

assign_to_message(
    InMessage = #{
        qos := Qos,
        topic := Topic,
        payload := Payload
    },
    Message
) ->
    NMsg = Message#message{qos = Qos, topic = Topic, payload = Payload},
    enrich_header(maps:get(headers, InMessage, #{}), NMsg).

enrich_header(Headers, Message) ->
    case maps:get(<<"allow_publish">>, Headers, undefined) of
        <<"false">> ->
            emqx_message:set_header(allow_publish, false, Message);
        <<"true">> ->
            emqx_message:set_header(allow_publish, true, Message);
        _ ->
            Message
    end.

topicfilters(Tfs) when is_list(Tfs) ->
    [
        #{name => emqx_topic:maybe_format_share(Topic), subopts => subopts(SubOpts)}
     || {Topic, SubOpts} <- Tfs
    ].

%% Apply the topic filters a `client.subscribe.rewrite' hook responded with.
%%
%% The hook runs after `check_sub_authzs'/`check_sub_caps' and its result goes
%% straight to the session, so what the broker guarantees about a SUBSCRIBE has
%% to be re-established here:
%%
%%   * one filter back per requested filter, in the same order, because SUBACK
%%     carries one reason code per requested topic filter, ordered as the
%%     filters were received (MQTT-3.9.3-1);
%%   * `subopts' within their MQTT ranges, since they end up as the granted QoS;
%%   * a rewritten filter authorized and within the zone's subscribe caps.
%%
%% A response breaking any of these is rejected as a whole: applying it in part
%% would subscribe the client to a set nobody asked for.
rewrite_topicfilters(ClientInfo, TopicFilters, RespTfs) when
    length(RespTfs) =:= length(TopicFilters)
->
    try
        {ok,
            lists:zipwith(
                fun(TopicFilter, RespTf) ->
                    rewrite_topicfilter(ClientInfo, TopicFilter, RespTf)
                end,
                TopicFilters,
                RespTfs
            )}
    catch
        throw:{Reason, Meta} ->
            ?SLOG(error, Meta#{
                msg => "exhook_topic_filter_rewrite_rejected",
                reason => Reason
            }),
            {error, Reason}
    end;
rewrite_topicfilters(_ClientInfo, TopicFilters, RespTfs) ->
    ?SLOG(error, #{
        msg => "exhook_topic_filter_rewrite_rejected",
        reason => topic_filter_count_mismatch,
        requested => length(TopicFilters),
        responded => length(RespTfs)
    }),
    {error, topic_filter_count_mismatch}.

rewrite_topicfilter(ClientInfo, TopicFilter = {Topic, SubOpts}, RespTf) ->
    NSubOpts = rewrite_subopts(SubOpts, maps:get(subopts, RespTf, undefined)),
    case rewrite_topic(Topic, NSubOpts, maps:get(name, RespTf, undefined)) of
        TopicFilter -> TopicFilter;
        NTopicFilter -> recheck_topicfilter(ClientInfo, NTopicFilter)
    end.

%% `topicfilters/1' flattens a shared subscription into its wire form
%% (`$share/<group>/<filter>'). Re-parse only a name the hook actually changed,
%% so an untouched filter keeps its original term: session subscriptions are
%% keyed by the `#share{}' record, and a flattened binary would no longer match
%% it on unsubscribe.
rewrite_topic(Topic, SubOpts, Name) when is_binary(Name) ->
    case emqx_topic:maybe_format_share(Topic) of
        Name ->
            {Topic, SubOpts};
        _ ->
            try
                true = emqx_topic:validate(filter, Name),
                emqx_topic:parse({Name, SubOpts})
            catch
                _:_ -> throw({invalid_topic_filter, #{topic => Name}})
            end
    end;
rewrite_topic(_Topic, _SubOpts, Name) ->
    throw({invalid_topic_filter, #{topic => Name}}).

%% A hook that only rewrites the topic can leave `subopts' unset, in which case
%% the client's own options stand. Whatever it does set is range-checked: these
%% become the granted QoS in the SUBACK, and an out-of-range value is either an
%% illegal reason code or, on MQTT 3.1.1, not serializable at all.
rewrite_subopts(SubOpts, undefined) ->
    SubOpts;
rewrite_subopts(SubOpts, NSubOpts) when is_map(NSubOpts) ->
    SubOpts#{
        qos => subopt(qos, NSubOpts, 0, 2),
        rh => subopt(rh, NSubOpts, 0, 2),
        rap => subopt(rap, NSubOpts, 0, 1),
        nl => subopt(nl, NSubOpts, 0, 1)
    };
rewrite_subopts(_SubOpts, NSubOpts) ->
    throw({invalid_subopts, #{subopts => NSubOpts}}).

subopt(Key, SubOpts, Min, Max) ->
    case maps:get(Key, SubOpts, Min) of
        Value when is_integer(Value), Value >= Min, Value =< Max ->
            Value;
        Value ->
            throw({invalid_subopts, #{Key => Value}})
    end.

%% `client.subscribe' runs once the channel has authorized the filters the
%% client asked for, so a rewritten one has never been checked. Re-run both
%% checks here, or a hook could hand the client a subscription it could not
%% have requested itself.
recheck_topicfilter(ClientInfo, {Topic, SubOpts = #{qos := Qos}}) ->
    AuthzContext = emqx_authz_context:make(ClientInfo),
    %% As in `emqx_channel', a shared subscription is authorized on its real topic
    case
        emqx_access_control:authorize(
            AuthzContext,
            ?AUTHZ_SUBSCRIBE(Qos),
            emqx_topic:get_shared_real_topic(Topic)
        )
    of
        deny ->
            throw({not_authorized, #{topic => emqx_topic:maybe_format_share(Topic)}});
        allow ->
            case emqx_mqtt_caps:check_sub(ClientInfo, Topic, SubOpts) of
                ok ->
                    {Topic, SubOpts};
                {ok, MaxQoS} ->
                    {Topic, SubOpts#{qos => MaxQoS}};
                {error, NRC} ->
                    throw(
                        {emqx_reason_codes:name(NRC), #{
                            topic => emqx_topic:maybe_format_share(Topic)
                        }}
                    )
            end
    end.

subopts(SubOpts) ->
    #{
        qos => maps:get(qos, SubOpts, 0),
        rh => maps:get(rh, SubOpts, 0),
        rap => maps:get(rap, SubOpts, 0),
        nl => maps:get(nl, SubOpts, 0)
    }.

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
    list_to_binary(inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256}));
ntoa(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).

option(undefined) -> <<>>;
option(B) -> B.

%% @private
stringfy(Term) when is_binary(Term) ->
    Term;
stringfy(Term) when is_integer(Term) ->
    integer_to_binary(Term);
stringfy(Term) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
stringfy(Term) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Term]))).

%%--------------------------------------------------------------------
%% Acc funcs

%% see exhook.proto
merge_responsed_ingress(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_ingress(Req, #{
    type := 'STOP_AND_RETURN', value := {bool_result, false}
}) ->
    {stop, Req#{result => false}};
merge_responsed_ingress(Req, #{type := Type, value := {message, NMessage}}) ->
    {ret(Type), Req#{message => NMessage}};
merge_responsed_ingress(Req, Resp) ->
    ?SLOG(warning, #{msg => "unknown_response_value", resp => Resp}),
    {stop, Req#{message => {error, {invalid_exhook_response, Resp}}}}.

merge_responsed_bool(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_bool(Req, #{type := Type, value := {bool_result, NewBool}}) when
    is_boolean(NewBool)
->
    {ret(Type), Req#{result => NewBool}};
merge_responsed_bool(_Req, Resp) ->
    ?SLOG(warning, #{msg => "unknown_response_value", resp => Resp}),
    ignore.

merge_responsed_message(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_message(Req, #{type := Type, value := {message, NMessage}}) ->
    {ret(Type), Req#{message => NMessage}};
merge_responsed_message(_Req, Resp) ->
    ?SLOG(warning, #{msg => "unknown_response_value", resp => Resp}),
    ignore.

merge_responsed_topicfilters(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_topicfilters(Req, #{
    type := Type, value := {topic_filters, #{filters := TopicFilters}}
}) when is_list(TopicFilters) ->
    {ret(Type), Req#{topic_filters => TopicFilters}};
%% A server registered as `client.subscribe' rather than `client.subscribe.rewrite'
%% answers `OnClientSubscribe' with an empty `EmptySuccess', which decodes to a
%% response carrying no value. Nothing to merge, and nothing worth warning about.
merge_responsed_topicfilters(_Req, #{value := undefined}) ->
    ignore;
merge_responsed_topicfilters(_Req, Resp) when map_size(Resp) =:= 0 ->
    ignore;
merge_responsed_topicfilters(_Req, #{type := 'CONTINUE'} = Resp) when map_size(Resp) =:= 1 ->
    ignore;
merge_responsed_topicfilters(_Req, Resp) ->
    ?SLOG(warning, #{msg => "unknown_response_value", resp => Resp}),
    ignore.

ret('CONTINUE') -> ok;
ret('STOP_AND_RETURN') -> stop.

request_meta() ->
    #{
        node => stringfy(node()),
        version => emqx_sys:version(),
        sysdescr => emqx_sys:sysdescr(),
        cluster_name => emqx_sys:cluster_name()
    }.
