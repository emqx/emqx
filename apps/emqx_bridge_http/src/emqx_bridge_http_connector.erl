%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_http_connector).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_trace.hrl").

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_query_async/4,
    on_get_status/2,
    on_get_status/3,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3,
    on_format_query_result/1
]).

-export([reply_delegator/3]).
-export([render_template/2]).

-export([
    roots/0,
    fields/1,
    desc/1,
    namespace/0
]).

%% for other http-like connectors.
-export([redact_request/1]).

-export([validate_method/1, join_paths/2, formalize_request/2, transform_result/1]).

-define(DEFAULT_PIPELINE_SIZE, 100).
-define(DEFAULT_REQUEST_TIMEOUT_MS, 30_000).

-define(READACT_REQUEST_NOTE, "the request body is redacted due to security reasons").

%%=====================================================================
%% Hocon schema

namespace() -> "connector_http".

roots() ->
    fields(config).

fields(config) ->
    [
        {connect_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    default => <<"15s">>,
                    desc => ?DESC("connect_timeout")
                }
            )},
        {max_retries,
            sc(
                non_neg_integer(),
                #{deprecated => {since, "5.0.4"}}
            )},
        {retry_interval,
            sc(
                emqx_schema:timeout_duration(),
                #{deprecated => {since, "5.0.4"}}
            )},
        {pool_type,
            sc(
                hoconsc:enum([random, hash]),
                #{
                    default => random,
                    desc => ?DESC("pool_type")
                }
            )},
        {pool_size,
            sc(
                pos_integer(),
                #{
                    default => 8,
                    desc => ?DESC("pool_size")
                }
            )},
        {enable_pipelining,
            sc(
                pos_integer(),
                #{
                    default => ?DEFAULT_PIPELINE_SIZE,
                    desc => ?DESC("enable_pipelining")
                }
            )},
        {request,
            hoconsc:mk(
                ref("request"),
                #{
                    default => undefined,
                    required => false,
                    desc => ?DESC("request")
                }
            )}
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("request") ->
    [
        {method,
            hoconsc:mk(binary(), #{
                required => false,
                desc => ?DESC("method"),
                validator => fun ?MODULE:validate_method/1
            })},
        {path, hoconsc:mk(emqx_schema:template(), #{required => false, desc => ?DESC("path")})},
        {body, hoconsc:mk(emqx_schema:template(), #{required => false, desc => ?DESC("body")})},
        {headers,
            hoconsc:mk(map(), #{required => false, desc => ?DESC("headers"), is_template => true})},
        {max_retries,
            sc(
                non_neg_integer(),
                #{
                    required => false,
                    desc => ?DESC("max_retries")
                }
            )},
        {request_timeout,
            sc(
                emqx_schema:timeout_duration_ms(),
                #{
                    required => false,
                    desc => ?DESC("request_timeout")
                }
            )}
    ].

desc(config) ->
    "";
desc("request") ->
    "";
desc(_) ->
    undefined.

validate_method(M) when
    M =:= <<"post">>;
    M =:= <<"put">>;
    M =:= <<"get">>;
    M =:= <<"delete">>;
    M =:= post;
    M =:= put;
    M =:= get;
    M =:= delete
->
    ok;
validate_method(M) ->
    case string:find(M, "${") of
        nomatch ->
            {error,
                <<"Invalid method, should be one of 'post', 'put', 'get', 'delete' or variables in ${field} format.">>};
        _ ->
            ok
    end.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).

%% ===================================================================
resource_type() -> webhook.

callback_mode() -> async_if_possible.

on_start(
    InstId,
    #{
        request_base := #{
            scheme := Scheme,
            host := Host,
            port := Port
        },
        connect_timeout := ConnectTimeout,
        pool_type := PoolType,
        pool_size := PoolSize
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_http_connector",
        connector => InstId,
        config => redact(Config)
    }),
    {Transport, TransportOpts} =
        case Scheme of
            http ->
                {tcp, []};
            https ->
                SSLConf = maps:get(ssl, Config),
                %% force enable ssl
                SSLOpts = emqx_tls_lib:to_client_opts(SSLConf#{enable => true}),
                {tls, SSLOpts}
        end,
    NTransportOpts = emqx_utils:ipv6_probe(TransportOpts),
    PoolOpts = [
        {host, Host},
        {port, Port},
        {connect_timeout, ConnectTimeout},
        {keepalive, 30000},
        {pool_type, PoolType},
        {pool_size, PoolSize},
        {transport, Transport},
        {transport_opts, NTransportOpts},
        {enable_pipelining, maps:get(enable_pipelining, Config, ?DEFAULT_PIPELINE_SIZE)}
    ],

    State = #{
        pool_name => InstId,
        pool_type => PoolType,
        host => Host,
        port => Port,
        connect_timeout => ConnectTimeout,
        scheme => Scheme,
        request => preprocess_request(maps:get(request, Config, undefined)),
        installed_actions => #{}
    },
    case start_pool(InstId, PoolOpts) of
        ok ->
            case do_get_status(InstId, ConnectTimeout) of
                ok ->
                    {ok, State};
                Error ->
                    ok = ehttpc_sup:stop_pool(InstId),
                    Error
            end;
        Error ->
            Error
    end.

start_pool(PoolName, PoolOpts) ->
    case ehttpc_sup:start_pool(PoolName, PoolOpts) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ?SLOG(warning, #{
                msg => "emqx_connector_on_start_already_started",
                connector => PoolName,
                pool_name => PoolName
            }),
            ok;
        Error ->
            Error
    end.

on_add_channel(
    _InstId,
    OldState,
    ActionId,
    ActionConfig
) ->
    InstalledActions = maps:get(installed_actions, OldState, #{}),
    {ok, ActionState} = do_create_http_action(ActionConfig),
    RenderTmplFunc = maps:get(render_template_func, ActionConfig, fun ?MODULE:render_template/2),
    ActionState1 = ActionState#{render_template_func => RenderTmplFunc},
    NewInstalledActions = maps:put(ActionId, ActionState1, InstalledActions),
    NewState = maps:put(installed_actions, NewInstalledActions, OldState),
    {ok, NewState}.

do_create_http_action(_ActionConfig = #{parameters := Params}) ->
    {ok, preprocess_request(Params)}.

on_stop(InstId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_http_connector",
        connector => InstId
    }),
    Res = ehttpc_sup:stop_pool(InstId),
    ?tp(emqx_connector_http_stopped, #{instance_id => InstId}),
    Res.

on_remove_channel(
    _InstId,
    OldState = #{installed_actions := InstalledActions},
    ActionId
) ->
    NewInstalledActions = maps:remove(ActionId, InstalledActions),
    NewState = maps:put(installed_actions, NewInstalledActions, OldState),
    {ok, NewState}.

%% BridgeV1 entrypoint
on_query(InstId, {send_message, Msg}, State) ->
    case maps:get(request, State, undefined) of
        undefined ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        Request ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request(Request, Msg),
            %% bridge buffer worker has retry, do not let ehttpc retry
            Retry = 2,
            ClientId = maps:get(clientid, Msg, undefined),
            on_query(
                InstId,
                {undefined, ClientId, Method, {Path, Headers, Body}, Timeout, Retry},
                State
            )
    end;
%% BridgeV2 entrypoint
on_query(
    InstId,
    {ActionId, Msg},
    State = #{installed_actions := InstalledActions}
) when is_binary(ActionId) ->
    case {maps:get(request, State, undefined), maps:get(ActionId, InstalledActions, undefined)} of
        {undefined, _} ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        {_, undefined} ->
            ?SLOG(error, #{msg => "action_not_found", connector => InstId, action_id => ActionId}),
            {error, action_not_found};
        {Request, ActionState} ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request_and_action(Request, ActionState, Msg),
            %% bridge buffer worker has retry, do not let ehttpc retry
            Retry = 2,
            ClientId = clientid(Msg),
            on_query(
                InstId,
                {ActionId, ClientId, Method, {Path, Headers, Body}, Timeout, Retry},
                State
            )
    end;
on_query(InstId, {Method, Request}, State) ->
    %% TODO: Get retry from State
    on_query(InstId, {undefined, undefined, Method, Request, 5000, _Retry = 2}, State);
on_query(InstId, {Method, Request, Timeout}, State) ->
    %% TODO: Get retry from State
    on_query(InstId, {undefined, undefined, Method, Request, Timeout, _Retry = 2}, State);
on_query(
    InstId,
    {ActionId, KeyOrNum, Method, Request, Timeout, Retry},
    #{host := Host, port := Port, scheme := Scheme} = State
) ->
    ?TRACE(
        "QUERY",
        "http_connector_received",
        #{
            request => redact_request(Request),
            note => ?READACT_REQUEST_NOTE,
            connector => InstId,
            action_id => ActionId,
            state => redact(State)
        }
    ),
    NRequest = formalize_request(Method, Request),
    trace_rendered_action_template(ActionId, Scheme, Host, Port, Method, NRequest, Timeout),
    Worker = resolve_pool_worker(State, KeyOrNum),
    Result0 = ehttpc:request(
        Worker,
        Method,
        NRequest,
        Timeout,
        Retry
    ),
    Result = transform_result(Result0),
    case Result of
        {error, {recoverable_error, Reason}} ->
            ?SLOG(warning, #{
                msg => "http_connector_do_request_failed",
                reason => Reason,
                connector => InstId
            }),
            {error, {recoverable_error, Reason}};
        {error, #{status_code := StatusCode}} ->
            ?SLOG(error, #{
                msg => "http_connector_do_request_received_error_response",
                note => ?READACT_REQUEST_NOTE,
                request => redact_request(NRequest),
                connector => InstId,
                status_code => StatusCode
            }),
            Result;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "http_connector_do_request_failed",
                note => ?READACT_REQUEST_NOTE,
                request => redact_request(NRequest),
                reason => Reason,
                connector => InstId
            }),
            Result;
        _Success ->
            Result
    end.

%% BridgeV1 entrypoint
on_query_async(InstId, {send_message, Msg}, ReplyFunAndArgs, State) ->
    case maps:get(request, State, undefined) of
        undefined ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        Request ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request(Request, Msg),
            ClientId = maps:get(clientid, Msg, undefined),
            on_query_async(
                InstId,
                {undefined, ClientId, Method, {Path, Headers, Body}, Timeout},
                ReplyFunAndArgs,
                State
            )
    end;
%% BridgeV2 entrypoint
on_query_async(
    InstId,
    {ActionId, Msg},
    ReplyFunAndArgs,
    State = #{installed_actions := InstalledActions}
) when is_binary(ActionId) ->
    case {maps:get(request, State, undefined), maps:get(ActionId, InstalledActions, undefined)} of
        {undefined, _} ->
            ?SLOG(error, #{msg => "arg_request_not_found", connector => InstId}),
            {error, arg_request_not_found};
        {_, undefined} ->
            ?SLOG(error, #{msg => "action_not_found", connector => InstId, action_id => ActionId}),
            {error, action_not_found};
        {Request, ActionState} ->
            #{
                method := Method,
                path := Path,
                body := Body,
                headers := Headers,
                request_timeout := Timeout
            } = process_request_and_action(Request, ActionState, Msg),
            ClientId = clientid(Msg),
            on_query_async(
                InstId,
                {ActionId, ClientId, Method, {Path, Headers, Body}, Timeout},
                ReplyFunAndArgs,
                State
            )
    end;
on_query_async(
    InstId,
    {ActionId, KeyOrNum, Method, Request, Timeout},
    ReplyFunAndArgs,
    #{host := Host, port := Port, scheme := Scheme} = State
) ->
    Worker = resolve_pool_worker(State, KeyOrNum),
    ?TRACE(
        "QUERY_ASYNC",
        "http_connector_received",
        #{
            request => redact_request(Request),
            note => ?READACT_REQUEST_NOTE,
            connector => InstId,
            state => redact(State)
        }
    ),
    NRequest = formalize_request(Method, Request),
    trace_rendered_action_template(ActionId, Scheme, Host, Port, Method, NRequest, Timeout),
    MaxAttempts = maps:get(max_attempts, State, 3),
    Context = #{
        attempt => 1,
        max_attempts => MaxAttempts,
        state => State,
        key_or_num => KeyOrNum,
        method => Method,
        request => NRequest,
        timeout => Timeout,
        trace_metadata => logger:get_process_metadata()
    },
    ok = ehttpc:request_async(
        Worker,
        Method,
        NRequest,
        Timeout,
        {fun ?MODULE:reply_delegator/3, [Context, ReplyFunAndArgs]}
    ),
    {ok, Worker}.

trace_rendered_action_template(ActionId, Scheme, Host, Port, Method, NRequest, Timeout) ->
    case NRequest of
        {Path, Headers} ->
            emqx_trace:rendered_action_template(
                ActionId,
                #{
                    host => Host,
                    port => Port,
                    path => Path,
                    method => Method,
                    headers => #emqx_trace_format_func_data{
                        function = fun emqx_utils_redact:redact_headers/1,
                        data = Headers
                    },
                    timeout => Timeout,
                    url => #emqx_trace_format_func_data{
                        function = fun render_url/1,
                        data = {Scheme, Host, Port, Path}
                    }
                }
            );
        {Path, Headers, Body} ->
            emqx_trace:rendered_action_template(
                ActionId,
                #{
                    host => Host,
                    port => Port,
                    path => Path,
                    method => Method,
                    headers => #emqx_trace_format_func_data{
                        function = fun emqx_utils_redact:redact_headers/1,
                        data = Headers
                    },
                    timeout => Timeout,
                    body => #emqx_trace_format_func_data{
                        function = fun log_format_body/1,
                        data = Body
                    },
                    url => #emqx_trace_format_func_data{
                        function = fun render_url/1,
                        data = {Scheme, Host, Port, Path}
                    }
                }
            )
    end.

render_url({Scheme, Host, Port, Path}) ->
    SchemeStr =
        case Scheme of
            http ->
                <<"http://">>;
            https ->
                <<"https://">>
        end,
    unicode:characters_to_binary([
        SchemeStr,
        Host,
        <<":">>,
        erlang:integer_to_binary(Port),
        Path
    ]).

log_format_body(Body) ->
    unicode:characters_to_binary(Body).

resolve_pool_worker(State, undefined) ->
    resolve_pool_worker(State, self());
resolve_pool_worker(#{pool_name := PoolName} = State, Key) ->
    case maps:get(pool_type, State, random) of
        random ->
            ehttpc_pool:pick_worker(PoolName);
        hash ->
            ehttpc_pool:pick_worker(PoolName, Key)
    end.

on_get_channels(ResId) ->
    emqx_bridge_v2:get_channels_for_connector(ResId).

on_get_status(InstId, State) ->
    on_get_status(InstId, State, fun default_health_checker/2).

on_get_status(InstId, #{pool_name := InstId, connect_timeout := Timeout}, DoPerWorker) ->
    case do_get_status(InstId, Timeout, DoPerWorker) of
        ok ->
            ?status_connected;
        {error, still_connecting} ->
            ?status_connecting;
        {error, Reason} ->
            {?status_disconnected, Reason}
    end.

do_get_status(PoolName, Timeout) ->
    do_get_status(PoolName, Timeout, fun default_health_checker/2).

do_get_status(PoolName, Timeout, DoPerWorker) ->
    Workers = [Worker || {_WorkerName, Worker} <- ehttpc:workers(PoolName)],
    try emqx_utils:pmap(fun(Worker) -> DoPerWorker(Worker, Timeout) end, Workers, Timeout) of
        [] ->
            {error, still_connecting};
        [_ | _] = Results ->
            case [E || {error, _} = E <- Results] of
                [] ->
                    ok;
                [{error, Reason} | _] ->
                    ?SLOG(info, #{
                        msg => "health_check_failed",
                        reason => redact(Reason),
                        connector => PoolName
                    }),
                    {error, Reason}
            end
    catch
        exit:timeout ->
            ?SLOG(info, #{
                msg => "health_check_failed",
                reason => timeout,
                connector => PoolName
            }),
            {error, timeout}
    end.

default_health_checker(Worker, Timeout) ->
    case ehttpc:health_check(Worker, Timeout) of
        ok ->
            ok;
        {error, _} = Error ->
            Error
    end.

on_get_channel_status(
    InstId,
    _ChannelId,
    State
) ->
    on_get_status(InstId, State, fun default_health_checker/2).

on_format_query_result({ok, Status, Headers, Body}) ->
    #{
        result => ok,
        response => #{
            status => Status,
            headers => Headers,
            body => Body
        }
    };
on_format_query_result({ok, Status, Headers}) ->
    #{
        result => ok,
        response => #{
            status => Status,
            headers => Headers
        }
    };
on_format_query_result(Result) ->
    Result.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
preprocess_request(undefined) ->
    undefined;
preprocess_request(Req) when map_size(Req) == 0 ->
    undefined;
preprocess_request(#{method := Method} = Req) ->
    Path = maps:get(path, Req, <<>>),
    Headers = maps:get(headers, Req, []),
    #{
        method => parse_template(to_bin(Method)),
        path => parse_template(Path),
        body => maybe_parse_template(body, Req),
        headers => parse_headers(Headers),
        request_timeout => maps:get(request_timeout, Req, ?DEFAULT_REQUEST_TIMEOUT_MS),
        max_retries => maps:get(max_retries, Req, 2)
    }.

parse_headers(Headers) when is_map(Headers) ->
    maps:fold(
        fun(K, V, Acc) -> [parse_header(K, V) | Acc] end,
        [],
        Headers
    );
parse_headers(Headers) when is_list(Headers) ->
    lists:map(
        fun({K, V}) -> parse_header(K, V) end,
        Headers
    ).

parse_header(K, V) ->
    KStr = to_bin(K),
    VTpl = parse_template(to_bin(V)),
    {parse_template(KStr), maybe_wrap_auth_header(KStr, VTpl)}.

maybe_wrap_auth_header(Key, VTpl) when
    (byte_size(Key) =:= 19 orelse byte_size(Key) =:= 13)
->
    %% We check the size of potential keys in the guard above and consider only
    %% those that match the number of characters of either "Authorization" or
    %% "Proxy-Authorization".
    case try_bin_to_lower(Key) of
        <<"authorization">> ->
            emqx_secret:wrap(VTpl);
        <<"proxy-authorization">> ->
            emqx_secret:wrap(VTpl);
        _Other ->
            VTpl
    end;
maybe_wrap_auth_header(_Key, VTpl) ->
    VTpl.

try_bin_to_lower(Bin) ->
    try iolist_to_binary(string:lowercase(Bin)) of
        LowercaseBin -> LowercaseBin
    catch
        _:_ -> Bin
    end.

maybe_parse_template(Key, Conf) ->
    case maps:get(Key, Conf, undefined) of
        undefined -> undefined;
        Val -> parse_template(Val)
    end.

parse_template(String) ->
    emqx_template:parse(String).

process_request_and_action(Request, ActionState, Msg) ->
    MethodTemplate = maps:get(method, ActionState),
    RenderTmplFunc = maps:get(render_template_func, ActionState),
    Method = make_method(render_template_string(MethodTemplate, RenderTmplFunc, Msg)),
    PathPrefix = unicode:characters_to_list(RenderTmplFunc(maps:get(path, Request), Msg)),
    PathSuffix = unicode:characters_to_list(RenderTmplFunc(maps:get(path, ActionState), Msg)),

    Path =
        case PathSuffix of
            "" -> PathPrefix;
            _ -> join_paths(PathPrefix, PathSuffix)
        end,

    ActionHaders = maps:get(headers, ActionState),
    BaseHeaders = maps:get(headers, Request),
    Headers = merge_headers(
        render_headers(ActionHaders, RenderTmplFunc, Msg),
        render_headers(BaseHeaders, RenderTmplFunc, Msg)
    ),
    BodyTemplate = maps:get(body, ActionState),
    Body = render_request_body(BodyTemplate, RenderTmplFunc, Msg),
    #{
        method => Method,
        path => Path,
        body => Body,
        headers => Headers,
        request_timeout => maps:get(request_timeout, ActionState)
    }.

merge_headers([], Result) ->
    Result;
merge_headers([{K, V} | Rest], Result) ->
    R = lists:keydelete(K, 1, Result),
    merge_headers(Rest, [{K, V} | R]).

process_request(
    #{
        method := MethodTemplate,
        path := PathTemplate,
        body := BodyTemplate,
        headers := HeadersTemplate,
        request_timeout := ReqTimeout
    } = Conf,
    Msg
) ->
    RenderTemplateFun = fun render_template/2,
    Conf#{
        method => make_method(render_template_string(MethodTemplate, RenderTemplateFun, Msg)),
        path => unicode:characters_to_list(RenderTemplateFun(PathTemplate, Msg)),
        body => render_request_body(BodyTemplate, RenderTemplateFun, Msg),
        headers => render_headers(HeadersTemplate, RenderTemplateFun, Msg),
        request_timeout => ReqTimeout
    }.

render_request_body(undefined, _, Msg) ->
    emqx_utils_json:encode(Msg);
render_request_body(BodyTks, RenderTmplFunc, Msg) ->
    RenderTmplFunc(BodyTks, Msg).

render_headers(HeaderTks, RenderTmplFunc, Msg) ->
    lists:map(
        fun({K, V}) ->
            {
                render_template_string(K, RenderTmplFunc, Msg),
                render_template_string(emqx_secret:unwrap(V), RenderTmplFunc, Msg)
            }
        end,
        HeaderTks
    ).

render_template(Template, Msg) ->
    % NOTE: ignoring errors here, missing variables will be rendered as `"undefined"`.
    {String, _Errors} = emqx_template:render(Template, {emqx_jsonish, Msg}),
    String.

render_template_string(Template, RenderTmplFunc, Msg) ->
    unicode:characters_to_binary(RenderTmplFunc(Template, Msg)).

make_method(M) when M == <<"POST">>; M == <<"post">> -> post;
make_method(M) when M == <<"PUT">>; M == <<"put">> -> put;
make_method(M) when M == <<"GET">>; M == <<"get">> -> get;
make_method(M) when M == <<"DELETE">>; M == <<"delete">> -> delete.

formalize_request(Method, Request) ->
    formalize_request(Method, "/", Request).

formalize_request(Method, BasePath, {Path, Headers, _Body}) when
    Method =:= get; Method =:= delete
->
    formalize_request(Method, BasePath, {Path, Headers});
formalize_request(_Method, BasePath, {Path, Headers, Body}) ->
    {join_paths(BasePath, Path), Headers, Body};
formalize_request(_Method, BasePath, {Path, Headers}) ->
    {join_paths(BasePath, Path), Headers}.

%% By default, we cannot treat HTTP paths as "file" or "resource" paths,
%% because an HTTP server may handle paths like
%% "/a/b/c/", "/a/b/c" and "/a//b/c" differently.
%%
%% So we try to avoid unnecessary path normalization.
%%
%% See also: `join_paths_test_/0`
join_paths(Path1, Path2) ->
    case is_start_with_question_mark(Path2) of
        true ->
            [Path1, Path2];
        false ->
            [without_trailing_slash(Path1), $/, without_starting_slash(Path2)]
    end.

is_start_with_question_mark([$? | _]) ->
    true;
is_start_with_question_mark(<<$?, _/binary>>) ->
    true;
is_start_with_question_mark(_) ->
    false.

without_starting_slash(Path) ->
    case do_without_starting_slash(Path) of
        empty -> <<>>;
        Other -> Other
    end.

do_without_starting_slash([]) ->
    empty;
do_without_starting_slash(<<>>) ->
    empty;
do_without_starting_slash([$/ | Rest]) ->
    Rest;
do_without_starting_slash([C | _Rest] = Path) when is_integer(C) andalso C =/= $/ ->
    Path;
do_without_starting_slash(<<$/, Rest/binary>>) ->
    Rest;
do_without_starting_slash(<<C, _Rest/binary>> = Path) when is_integer(C) andalso C =/= $/ ->
    Path;
%% On actual lists the recursion should very quickly exhaust
do_without_starting_slash([El | Rest]) ->
    case do_without_starting_slash(El) of
        empty -> do_without_starting_slash(Rest);
        ElRest -> [ElRest | Rest]
    end.

without_trailing_slash(Path) ->
    case iolist_to_binary(Path) of
        <<>> ->
            <<>>;
        B ->
            case binary:last(B) of
                $/ -> binary_part(B, 0, byte_size(B) - 1);
                _ -> B
            end
    end.

to_bin(Bin) when is_binary(Bin) ->
    Bin;
to_bin(Str) when is_list(Str) ->
    list_to_binary(Str);
to_bin(Atom) when is_atom(Atom) ->
    atom_to_binary(Atom, utf8).

reply_delegator(
    #{trace_metadata := TraceMetadata} = Context,
    ReplyFunAndArgs,
    Result0
) ->
    spawn(fun() ->
        logger:set_process_metadata(TraceMetadata),
        Result = transform_result(Result0),
        logger:unset_process_metadata(),
        maybe_retry(Result, Context, ReplyFunAndArgs)
    end).

transform_result(Result) ->
    case Result of
        %% The normal reason happens when the HTTP connection times out before
        %% the request has been fully processed
        {error, Reason} when
            Reason =:= econnrefused;
            Reason =:= timeout;
            Reason =:= normal;
            Reason =:= {shutdown, normal};
            Reason =:= {shutdown, closed}
        ->
            {error, {recoverable_error, Reason}};
        {error, {closed, _Message} = Reason} ->
            %% _Message = "The connection was lost."
            {error, {recoverable_error, Reason}};
        {error, _Reason} ->
            Result;
        {ok, StatusCode, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            Result;
        {ok, StatusCode, _, _} when StatusCode >= 200 andalso StatusCode < 300 ->
            Result;
        {ok, _TooManyRequests = StatusCode = 429, Headers} ->
            {error, {recoverable_error, #{status_code => StatusCode, headers => Headers}}};
        {ok, _ServiceUnavailable = StatusCode = 503, Headers} ->
            {error, {recoverable_error, #{status_code => StatusCode, headers => Headers}}};
        {ok, StatusCode, Headers} ->
            {error, {unrecoverable_error, #{status_code => StatusCode, headers => Headers}}};
        {ok, _TooManyRequests = StatusCode = 429, Headers, Body} ->
            {error,
                {recoverable_error, #{
                    status_code => StatusCode, headers => Headers, body => Body
                }}};
        {ok, _ServiceUnavailable = StatusCode = 503, Headers, Body} ->
            {error,
                {recoverable_error, #{
                    status_code => StatusCode, headers => Headers, body => Body
                }}};
        {ok, StatusCode, Headers, Body} ->
            {error,
                {unrecoverable_error, #{
                    status_code => StatusCode, headers => Headers, body => Body
                }}}
    end.

maybe_retry(Result, _Context = #{attempt := N, max_attempts := Max}, ReplyFunAndArgs) when
    N >= Max
->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
maybe_retry(
    {error, {unrecoverable_error, #{status_code := _}}} = Result, _Context, ReplyFunAndArgs
) ->
    %% request was successful, but we got an error response; no need to retry
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result);
maybe_retry({error, Reason}, Context, ReplyFunAndArgs) ->
    #{
        state := State,
        attempt := Attempt,
        key_or_num := KeyOrNum,
        method := Method,
        request := Request,
        timeout := Timeout
    } = Context,
    %% TODO: reset the expiration time for free retries?
    IsFreeRetry =
        case Reason of
            {recoverable_error, normal} -> true;
            {recoverable_error, {shutdown, normal}} -> true;
            _ -> false
        end,
    NContext =
        case IsFreeRetry of
            true -> Context;
            false -> Context#{attempt := Attempt + 1}
        end,
    ?tp(http_will_retry_async, #{}),
    Worker = resolve_pool_worker(State, KeyOrNum),
    ok = ehttpc:request_async(
        Worker,
        Method,
        Request,
        Timeout,
        {fun ?MODULE:reply_delegator/3, [NContext, ReplyFunAndArgs]}
    ),
    ok;
maybe_retry(Result, _Context, ReplyFunAndArgs) ->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, Result).

%% Function that will do a deep traversal of Data and remove sensitive
%% information (i.e., passwords)
redact(Data) ->
    emqx_utils:redact(Data).

%% because the body may contain some sensitive data
%% and at the same time the redact function will not scan the binary data
%% and we also can't know the body format and where the sensitive data will be
%% so the easy way to keep data security is redacted the whole body
redact_request({Path, Headers}) ->
    {Path, emqx_utils_redact:redact_headers(Headers)};
redact_request({Path, Headers, _Body}) ->
    {Path, emqx_utils_redact:redact_headers(Headers), <<"******">>}.

clientid(Msg) -> maps:get(clientid, Msg, undefined).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

iolists_equal(L1, L2) ->
    iolist_to_binary(L1) =:= iolist_to_binary(L2).

redact_test_() ->
    TestData = #{
        headers => [
            {<<"content-type">>, <<"application/json">>},
            {<<"Authorization">>, <<"Basic YWxhZGRpbjpvcGVuc2VzYW1l">>}
        ]
    },
    [
        ?_assertNotEqual(TestData, redact(TestData))
    ].

join_paths_test_() ->
    [
        ?_assert(iolists_equal("abc/cde", join_paths("abc", "cde"))),
        ?_assert(iolists_equal("abc/cde", join_paths(<<"abc">>, <<"cde">>))),
        ?_assert(
            iolists_equal(
                "abc/cde",
                join_paths([["a"], <<"b">>, <<"c">>], [
                    [[[], <<>>], <<>>, <<"c">>], <<"d">>, <<"e">>
                ])
            )
        ),

        ?_assert(iolists_equal("abc/cde", join_paths("abc", "/cde"))),
        ?_assert(iolists_equal("abc/cde", join_paths(<<"abc">>, <<"/cde">>))),
        ?_assert(
            iolists_equal(
                "abc/cde",
                join_paths([["a"], <<"b">>, <<"c">>], [
                    [<<>>, [[], <<>>], <<"/c">>], <<"d">>, <<"e">>
                ])
            )
        ),

        ?_assert(iolists_equal("abc/cde", join_paths("abc/", "cde"))),
        ?_assert(iolists_equal("abc/cde", join_paths(<<"abc/">>, <<"cde">>))),
        ?_assert(
            iolists_equal(
                "abc/cde",
                join_paths([["a"], <<"b">>, <<"c">>, [<<"/">>]], [
                    [[[], [], <<>>], <<>>, [], <<"c">>], <<"d">>, <<"e">>
                ])
            )
        ),

        ?_assert(iolists_equal("abc/cde", join_paths("abc/", "/cde"))),
        ?_assert(iolists_equal("abc/cde", join_paths(<<"abc/">>, <<"/cde">>))),
        ?_assert(
            iolists_equal(
                "abc/cde",
                join_paths([["a"], <<"b">>, <<"c">>, [<<"/">>]], [
                    [[[], <<>>], <<>>, [[$/]], <<"c">>], <<"d">>, <<"e">>
                ])
            )
        ),

        ?_assert(iolists_equal("/", join_paths("", ""))),
        ?_assert(iolists_equal("/cde", join_paths("", "cde"))),
        ?_assert(iolists_equal("/cde", join_paths("", "/cde"))),
        ?_assert(iolists_equal("/cde", join_paths("/", "cde"))),
        ?_assert(iolists_equal("/cde", join_paths("/", "/cde"))),
        ?_assert(iolists_equal("//cde/", join_paths("/", "//cde/"))),
        ?_assert(iolists_equal("abc///cde/", join_paths("abc//", "//cde/"))),
        ?_assert(iolists_equal("abc?v=1", join_paths("abc", "?v=1"))),
        ?_assert(iolists_equal("abc?v=1", join_paths("abc", <<"?v=1">>))),
        ?_assert(iolists_equal("abc/?v=1", join_paths("abc/", <<"?v=1">>)))
    ].

-endif.
