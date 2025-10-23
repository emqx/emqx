%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

on_client_authorize(ClientInfo, Action, Topic, Result) ->
    Bool = maps:get(result, Result, deny) == allow,
    %% TODO: Support full action in major release
    Type =
        case Action of
            ?authz_action(publish) -> 'PUBLISH';
            ?authz_action(subscribe) -> 'SUBSCRIBE'
        end,
    Req = #{
        clientinfo => clientinfo(ClientInfo),
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
    emqx_exhook:cast('client.subscribe', Req).

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
merge_responsed_bool(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_bool(Req, #{type := Type, value := {bool_result, NewBool}}) when
    is_boolean(NewBool)
->
    {ret(Type), Req#{result => NewBool}};
merge_responsed_bool(_Req, Resp) ->
    ?SLOG(warning, #{msg => "unknown_responsed_value", resp => Resp}),
    ignore.

merge_responsed_message(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_message(Req, #{type := Type, value := {message, NMessage}}) ->
    {ret(Type), Req#{message => NMessage}};
merge_responsed_message(_Req, Resp) ->
    ?SLOG(warning, #{msg => "unknown_responsed_value", resp => Resp}),
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
