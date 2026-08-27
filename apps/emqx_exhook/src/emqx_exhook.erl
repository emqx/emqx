%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_exhook).

-include("emqx_exhook.hrl").
-include_lib("emqx/include/logger.hrl").

-export([
    cast/2,
    call_fold/3
]).

%% exported for `emqx_telemetry'
-export([get_basic_usage_info/0]).

%%--------------------------------------------------------------------
%% Dispatch APIs
%%--------------------------------------------------------------------

-spec cast(atom(), map()) -> ok.
cast(Hookpoint, Req) ->
    cast(Hookpoint, Req, emqx_exhook_mgr:running()).

cast(_, _, []) ->
    ok;
cast(Hookpoint, Req, [ServerName | More]) ->
    %% XXX: Need a real asynchronous running
    _ = emqx_exhook_server:call(
        Hookpoint,
        Req,
        emqx_exhook_mgr:service(ServerName)
    ),
    cast(Hookpoint, Req, More).

-spec call_fold(atom(), term(), function()) ->
    {ok, term()}
    | {stop, term()}
    | ignore.
call_fold(Hookpoint, Req, AccFun) ->
    case emqx_exhook_mgr:running() of
        [] ->
            no_running_server_result(Hookpoint, Req);
        ServerNames ->
            call_fold(true, Hookpoint, Req, AccFun, ServerNames)
    end.

no_running_server_result(Hookpoint, Req) ->
    case no_running_server_action() of
        ignore ->
            ignore;
        deny ->
            {stop, deny_action_result(Hookpoint, Req)}
    end.

no_running_server_action() ->
    case emqx_security_profile:policy(exhook_server_unavailable) of
        deny ->
            deny;
        honor_failed_action ->
            configured_failed_action()
    end.

configured_failed_action() ->
    Servers = emqx:get_config([exhook, servers], []),
    case
        lists:any(
            fun(#{enable := Enable, failed_action := FailedAction}) ->
                Enable andalso FailedAction =:= deny
            end,
            Servers
        )
    of
        true -> deny;
        false -> ignore
    end.

call_fold(true, _, _Req, _, []) ->
    ignore;
call_fold(false, _, Req, _, []) ->
    {ok, Req};
call_fold(IsIgnore, Hookpoint, Req, AccFun, [ServerName | More]) ->
    Server = emqx_exhook_mgr:service(ServerName),
    case emqx_exhook_server:call(Hookpoint, Req, Server) of
        {ok, Resp} ->
            case AccFun(Req, Resp) of
                {stop, NReq} ->
                    {stop, NReq};
                {ok, NReq} ->
                    call_fold(false, Hookpoint, NReq, AccFun, More);
                ignore ->
                    call_fold(IsIgnore, Hookpoint, Req, AccFun, More)
            end;
        _ ->
            case emqx_exhook_server:failed_action(Server) of
                ignore ->
                    call_fold(IsIgnore, Hookpoint, Req, AccFun, More);
                deny ->
                    {stop, deny_action_result(Hookpoint, Req)}
            end
    end.

%% XXX: Hard-coded the deny response
deny_action_result('client.authenticate', _) ->
    #{result => false};
deny_action_result('client.authorize', _) ->
    #{result => false};
%% The hook may only rewrite topic filters, so denying it means: do not rewrite.
%% What the client asked for has already been authorized by the channel.
deny_action_result('client.subscribe', Req) ->
    Req;
deny_action_result('message.ingress', _) ->
    #{result => false};
deny_action_result('message.publish', Req) ->
    case emqx_security_profile:policy(exhook_message_publish_failure) of
        ignore ->
            Req;
        deny ->
            #{message := Message} = Req,
            Headers = maps:get(headers, Message, #{}),
            Req#{message := Message#{headers => Headers#{<<"allow_publish">> => <<"false">>}}}
    end.

%%--------------------------------------------------------------------
%% APIs for `emqx_telemetry'
%%--------------------------------------------------------------------

-spec get_basic_usage_info() ->
    #{
        num_servers => non_neg_integer(),
        servers =>
            [
                #{
                    driver => Driver,
                    hooks => [emqx_exhook_server:hookpoint()]
                }
            ]
    }
when
    Driver :: grpc.
get_basic_usage_info() ->
    try
        Servers = emqx_exhook_mgr:running(),
        NumServers = length(Servers),
        ServerInfo =
            lists:map(
                fun(ServerName) ->
                    Hooks = emqx_exhook_mgr:hooks(ServerName),
                    HookNames = lists:map(fun(#{name := Name}) -> Name end, Hooks),
                    #{
                        hooks => HookNames,
                        %% currently, only grpc driver exists.
                        driver => grpc
                    }
                end,
                Servers
            ),
        #{
            num_servers => NumServers,
            servers => ServerInfo
        }
    catch
        _:_ ->
            #{
                num_servers => 0,
                servers => []
            }
    end.
