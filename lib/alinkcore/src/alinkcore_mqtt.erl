%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 11. 3月 2023 下午8:05
%%%-------------------------------------------------------------------
-module(alinkcore_mqtt).
-author("yqfclid").

-include("alinkcore.hrl").

-export([
    start/0,
    stop/0
]).

%% Client Lifecircle Hooks
-export([
    on_client_connect/3,
    on_client_connack/4,
    on_client_connected/3,
    on_client_disconnected/4,
    on_client_authenticate/3,
    on_client_authorize/5,
    on_client_subscribe/4,
    on_client_unsubscribe/4
]).

%% Session Lifecircle Hooks
-export([
    on_session_created/3,
    on_session_subscribed/4,
    on_session_unsubscribed/4,
    on_session_resumed/3,
    on_session_discarded/3,
    on_session_takeovered/3,
    on_session_terminated/4
]).

%% Message Pubsub Hooks
-export([
    on_message_publish/2,
    on_message_delivered/3,
    on_message_acked/3,
    on_message_dropped/4
]).

%% Called when the plugin application start
start() ->
    Env = [],
    hook('client.connect',      {?MODULE, on_client_connect, [Env]}),
    hook('client.connack',      {?MODULE, on_client_connack, [Env]}),
    hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
    hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    hook('client.authenticate', {?MODULE, on_client_authenticate, [Env]}),
    hook('client.authorize',    {?MODULE, on_client_authorize, [Env]}),
    hook('client.subscribe',    {?MODULE, on_client_subscribe, [Env]}),
    hook('client.unsubscribe',  {?MODULE, on_client_unsubscribe, [Env]}),
    hook('session.created',     {?MODULE, on_session_created, [Env]}),
    hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    hook('session.resumed',     {?MODULE, on_session_resumed, [Env]}),
    hook('session.discarded',   {?MODULE, on_session_discarded, [Env]}),
    hook('session.takeovered',  {?MODULE, on_session_takeovered, [Env]}),
    hook('session.terminated',  {?MODULE, on_session_terminated, [Env]}),
    hook('message.publish',     {?MODULE, on_message_publish, [Env]}),
    hook('message.delivered',   {?MODULE, on_message_delivered, [Env]}),
    hook('message.acked',       {?MODULE, on_message_acked, [Env]}),
    hook('message.dropped',     {?MODULE, on_message_dropped, [Env]}).

%%--------------------------------------------------------------------
%% Client LifeCircle Hooks
%%--------------------------------------------------------------------

on_client_connect(ConnInfo, Props, _Env) ->
    %% this is to demo the usage of EMQX's structured-logging macro
    %% * Recommended to always have a `msg` field,
    %% * Use underscore instead of space to help log indexers,
    %% * Try to use static fields
    #{peername := {RemoteIp, _}} = ConnInfo,
    Ip = alinkutil_type:to_binary(inet:ntoa(RemoteIp)),
    erlang:put(alink_remote_ip, Ip),
    {ok, Props}.

on_client_connack(_ConnInfo = #{clientid := _ClientId}, _Rc, Props, _Env) ->
    {ok, Props}.

on_client_connected(_ClientInfo = #{clientid := _ClientId}, _ConnInfo, _Env) ->
    case erlang:get(alink_addr) of
        undefined ->
            ok;
        Addr ->
            Session = #session{node = node(), pid = self(), connect_time = erlang:system_time(second)},
            erlang:put(alink_session, Session),
            alinkcore_cache:save(session, Addr, Session)
    end,
    ok.

on_client_disconnected(_ClientInfo = #{clientid := _ClientId}, ReasonCode, _ConnInfo, _Env) ->
    case erlang:get(alink_addr) of
        undefined ->
            ok;
        Addr ->
            alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"logout">>}]),
            ProductId = erlang:get(alink_product_id),
            Ip =
                case erlang:get(alink_remote_ip) of
                    undefined ->
                        <<>>;
                    I ->
                        I
                end,
            alinkcore_device_log:disconnected(Addr, ProductId, ReasonCode, Ip),
            Session = erlang:get(alink_session),
            alinkcore_cache:delete_object(session, Addr, Session),
            Mod = erlang:get(alinkcore_protocol_mod),
            case erlang:function_exported(Mod, handle_close, 3) of
                false ->
                    ok;
                true ->
                    try
                        Mod:handle_close(ProductId, Addr, Session)
                    catch
                        E:R:ST ->
                            logger:error("handle ~p close failed:~p ~p ~p", [Addr, E, R, ST])
                    end
            end,
            erase_device_info()
    end,
    ok.

on_client_authenticate(#{clientid := <<"uzy_1_", Addr/binary>>} = ConnInfo, Result, _Env) ->
    #{username := UserName, password := Password} = ConnInfo,
    Auth =
        case catch auth_by_device(Addr, UserName, Password, ConnInfo) of
            ok ->
                alinkcore_device_event:online(Addr),
                alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"login">>}]),
                success;
            {error, Reason} ->
                logger:error("~p auth failed ~p", [Addr, Reason]),
                bad_username_or_password
        end,
    {ok, Result#{auth_result=> Auth}};
on_client_authenticate(#{clientid := <<"uzy_2_", Addr/binary>>} = ConnInfo, Result, _Env) ->
    #{username := UserName, password := Password} = ConnInfo,
    Auth =
        case catch auth_by_product(Addr, UserName, Password, ConnInfo) of
            ok ->
                alinkcore_device_event:online(Addr),
                alinkdata_hooks:run('alinkiot.metrics', [#{event => <<"device_metrics">>, type => <<"login">>}]),
                success;
            {error, Reason} ->
                logger:error("~p auth failed ~p", [Addr, Reason]),
                bad_username_or_password
        end,
    {ok, Result#{auth_result=> Auth}};
on_client_authenticate(#{clientid := <<"ws_", _/binary>>} = ConnInfo, Result, _Env) ->
    #{username := UserName, password := Token} = ConnInfo,
    Auth =
        case ehttpd_auth:get_session(Token) of
            #{<<"user">> := #{<<"userName">> := UserName}} ->
                success;
            _ ->
                bad_username_or_password
        end,
    {ok, Result#{auth_result=> Auth}};
on_client_authenticate(#{clientid := <<"user_", _/binary>>} = ConnInfo, Result, _Env) ->
    #{username := UserName, password := Token} = ConnInfo,
    Auth =
        case ehttpd_auth:get_session(Token) of
            #{<<"user">> := #{<<"userName">> := UserName}} ->
                success;
            _ ->
                bad_username_or_password
        end,
    {ok, Result#{auth_result=> Auth}};
on_client_authenticate(#{clientid := <<"app_", _/binary>>} = ConnInfo, Result, _Env) ->
    #{username := AppId, password := Token} = ConnInfo,
    erlang:put(alink_appid, AppId),
    Auth =
        case ehttpd_auth:get_session(Token) of
            #{<<"appid">> := AppId} ->
                success;
            _ ->
                bad_username_or_password
        end,
    {ok, Result#{auth_result=> Auth}};
on_client_authenticate(_ConnInfo, Result, _Env) ->
    {ok, Result#{auth_result=> bad_username_or_password}}.

on_client_authorize(_ClientInfo = #{clientid := _ClientId}, _PubSub, _Topic, Result, _Env) ->
    {ok, Result}.

on_client_subscribe(#{clientid := _ClientId}, _Properties, TopicFilters, _Env) ->
    {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := _ClientId}, _Properties, TopicFilters, _Env) ->
    {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session LifeCircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := _ClientId}, _SessInfo, _Env) ->
    ok.

on_session_subscribed(#{clientid := _ClientId}, _Topic, _SubOpts, _Env) ->
    ok.

on_session_unsubscribed(#{clientid := _ClientId}, _Topic, _Opts, _Env) ->
    ok.

on_session_resumed(#{clientid := _ClientId}, _SessInfo, _Env) ->
    ok.

on_session_discarded(_ClientInfo = #{clientid := _ClientId}, _SessInfo, _Env) ->
    ok.

on_session_takeovered(_ClientInfo = #{clientid := ClientId}, SessInfo, _Env) ->
    logger:debug("Session(~s) is takeovered. Session Info: ~p~n", [ClientId, SessInfo]).

on_session_terminated(_ClientInfo = #{clientid := ClientId}, Reason, SessInfo, _Env) ->
    logger:debug("Session(~s) is terminated due to ~p~nSession Info: ~p~n",
        [ClientId, Reason, SessInfo]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message = #message{topic = <<"p/in">>, payload = Payload}, _Env) ->
    case jiffy:decode(Payload, [return_maps]) of
        #{<<"msgType">> := <<"sync">>, <<"data">> := #{<<"status">> := Status}} ->
            case erlang:get(alink_appid) of
                undefined ->
                    ok;
                AppId ->
                    alinkdata_app_service:set_sync_status(AppId, Status)
            end;
        #{
            <<"msgType">> := <<"control_device">>,
            <<"data">> := #{
                <<"addr">> := Addr,
                <<"type">> := WriteType,
                <<"name">> := Name,
                <<"value">> := Value
            }
        } ->
            case alinkcore_cache:lookup(session, Addr) of
                {ok, Session} ->
                    NData =
                        #{
                            <<"type">> => WriteType,
                            <<"name">> => Name,
                            <<"value">> => Value
                        },
                    NTopic = <<"p/in/", Addr/binary>>,
                    alinkcore_message_forward:forward(Session, Message#message{payload = jiffy:encode(NData), topic = NTopic});
                undefined ->
                    ok
            end;
        _ ->
            ok
    end,
    {ok, Message};
on_message_publish(Message = #message{topic = <<"p/in/", Addr/binary>>}, _Env) ->
    case alinkcore_cache:lookup(session, Addr) of
        {ok, Session} ->
            alinkcore_message_forward:forward(Session, Message);
        undefined ->
            ok
    end,
    {ok, Message};
on_message_publish(Message = #message{topic = <<"d/out/", _/binary>>}, _Env) ->
    case erlang:get(alinkcore_protocol_mod) of
        undefined ->
            ok;
        Mod ->
            ProductId = erlang:get(alink_product_id),
            Addr = erlang:get(alink_addr),
            Ip =
                case erlang:get(alink_remote_ip) of
                    undefined ->
                        <<>>;
                    I ->
                        I
                end,
            alinkcore_device_log:up_data(Addr, ProductId, Message#message.payload, Ip),
            Mod:handle_in(ProductId, Addr, emqx_message:to_map(Message))
    end,
    {ok, Message};

on_message_publish(Message = #message{topic = <<"$dp">>}, _Env) ->
    case erlang:get(alinkcore_protocol_mod) of
        gnss_server ->
            ProductId = erlang:get(alink_product_id),
            Addr = erlang:get(alink_addr),
            Ip =
                case erlang:get(alink_remote_ip) of
                    undefined ->
                        <<>>;
                    I ->
                        I
                end,
            <<_:3/binary, RealPayload/binary>> = Message#message.payload,
            alinkcore_device_log:up_data(Addr, ProductId, RealPayload, Ip),
            gnss_server:handle_in(ProductId, Addr, emqx_message:to_map(Message));
        _ ->
            ok
    end,
    {ok, Message};
on_message_publish(Message = #message{topic = <<"s/in/", _/binary>>}, _Env) ->
    case erlang:get(alink_appid) of
        undefined ->
            ok;
        AppId ->
            case jiffy:decode(Message#message.payload, [return_maps]) of
                #{<<"msgType">> := <<"sync">>, <<"data">> := Status} ->
                    alinkdata_app_service:set_sync_status(AppId, Status);
                _ ->
                    ok
            end
    end,
    {ok, Message};

on_message_publish(Message, _Env) ->
    {ok, Message}.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Env) ->
    ok;
on_message_dropped(_Message, _By = #{node := _Node}, _Reason, _Env) ->
    ok.



on_message_delivered(_ClientInfo, #message{topic = <<"p/in/", _/binary>>} = Message, _Env) ->
    Mod = erlang:get(alinkcore_protocol_mod),
    ProductId = erlang:get(alink_product_id),
    Addr = erlang:get(alink_addr),
    Message0 =
        case erlang:function_exported(Mod, handle_out, 3) of
            false ->
                Message;
            true ->
                Mod:handle_out(ProductId, Addr, emqx_message:to_map(Message))
        end,
    Ip =
        case erlang:get(alink_remote_ip) of
            undefined ->
                <<>>;
            I ->
                I
        end,
    alinkcore_device_log:down_data(Addr, ProductId, Message0#message.payload, Ip),
    {ok, Message0};
on_message_delivered(_ClientInfo = #{clientid := _ClientId}, Message, _Env) ->
    {ok, Message}.

on_message_acked(_ClientInfo = #{clientid := _ClientId}, _Message, _Env) ->
    ok.

%% Called when the plugin application stop
stop() ->
    unhook('client.connect',      {?MODULE, on_client_connect}),
    unhook('client.connack',      {?MODULE, on_client_connack}),
    unhook('client.connected',    {?MODULE, on_client_connected}),
    unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    unhook('client.authenticate', {?MODULE, on_client_authenticate}),
    unhook('client.authorize',    {?MODULE, on_client_authorize}),
    unhook('client.subscribe',    {?MODULE, on_client_subscribe}),
    unhook('client.unsubscribe',  {?MODULE, on_client_unsubscribe}),
    unhook('session.created',     {?MODULE, on_session_created}),
    unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    unhook('session.resumed',     {?MODULE, on_session_resumed}),
    unhook('session.discarded',   {?MODULE, on_session_discarded}),
    unhook('session.takeovered',  {?MODULE, on_session_takeovered}),
    unhook('session.terminated',  {?MODULE, on_session_terminated}),
    unhook('message.publish',     {?MODULE, on_message_publish}),
    unhook('message.delivered',   {?MODULE, on_message_delivered}),
    unhook('message.acked',       {?MODULE, on_message_acked}),
    unhook('message.dropped',     {?MODULE, on_message_dropped}).

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).



auth_by_device(Addr, UserName, Password, ConnInfo) ->
    case alinkcore_cache:auth_by_device(Addr, UserName, Password) of
        true ->
            case load_device_info(Addr, ConnInfo) of
                ok ->
                    ok;
                {error, Reason} ->
                    logger:error("load device info error ~p", [Reason]),
                    {error, Reason}
            end;
        false ->
            {error, deny};
        Err ->
            logger:error("auth failed ~p", [Err]),
            {error, Err}
    end.


auth_by_product(Addr, UserName, Password, ConnInfo) ->
    case alinkcore_cache:auth_by_product(Addr, UserName, Password) of
        true ->
            case load_device_info(Addr, ConnInfo) of
                ok ->
                    ok;
                {error, Reason} ->
                    logger:error("load device info error ~p", [Reason]),
                    {error, Reason}
            end;
        false ->
            {error, deny};
        Err ->
            logger:error("auth failed ~p", [Err]),
            {error, Err}
    end.

load_device_info(Addr, ConnInfo) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"product">> := ProductId}} ->
            case alinkcore_cache:query_product(ProductId) of
                {ok, #{<<"protocol">> := Protocol}} ->
                    case alinkcore_protocol:get_mod(Protocol) of
                        undefined ->
                            {error, deny};
                        Mod ->
                            case Mod:init([Addr, ConnInfo]) of
                                ok ->
                                    erlang:put(alinkcore_protocol_mod, Mod),
                                    erlang:put(alink_product_id, ProductId),
                                    erlang:put(alink_addr, Addr),
                                    ok;
                                Other ->
                                    {error, Other}
                            end
                    end;
                {error, Reason} ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


erase_device_info() ->
    erlang:erase(alink_product_id),
    erlang:erase(alinkcore_protocol_mod),
    erlang:erase(alink_addr).
