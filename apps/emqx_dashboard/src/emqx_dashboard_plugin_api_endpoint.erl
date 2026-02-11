%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_plugin_api_endpoint).

-behaviour(minirest_api).

-export([api_spec/0, paths/0, schema/1]).
-export([gateway/3]).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DEFAULT_TIMEOUT, 5000).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    ["/plugin_api/:plugin/[...]"].

schema("/plugin_api/:plugin/[...]") ->
    #{
        'operationId' => gateway,
        get => gateway_schema(),
        post => gateway_schema(),
        put => gateway_schema(),
        patch => gateway_schema(),
        delete => gateway_schema()
    }.

gateway_schema() ->
    #{
        responses => #{
            200 => hoconsc:mk(map()),
            400 => hoconsc:mk(map()),
            401 => hoconsc:mk(map()),
            404 => hoconsc:mk(map()),
            500 => hoconsc:mk(map()),
            503 => hoconsc:mk(map())
        }
    }.

gateway(Method, Params, Request) ->
    Bindings = maps:get(bindings, Params, #{}),
    {PluginFromPath, PathRemainder0} = parse_request_path(Request),
    Plugin = maps:get(plugin, Bindings, PluginFromPath),
    PathRemainder =
        case parse_bindings_path_remainder(Bindings) of
            [] -> PathRemainder0;
            R -> R
        end,
    Headers = maps:get(headers, Params, #{}),
    ReqInfo = #{
        method => Method,
        query_string => maps:get(query_string, Params, #{}),
        headers => Headers,
        body => maps:get(body, Params, #{})
    },
    AuthMeta = maps:get(auth_meta, Params, #{}),
    Context = #{
        auth_meta => AuthMeta,
        namespace => emqx_dashboard:get_namespace(Params)
    },
    case Plugin of
        undefined ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Plugin API Not Found">>}};
        _ ->
            call_plugin_api(Plugin, Method, PathRemainder, ReqInfo, Context)
    end.

call_plugin_api(Plugin, Method, PathRemainder, ReqInfo, Context) ->
    case emqx_plugins:resolve_api_callback(Plugin) of
        {ok, CallbackModule} ->
            Timeout = emqx:get_config(
                [plugins, api_endpoint, timeout],
                emqx:get_config([plugins, api_gateway, timeout], ?DEFAULT_TIMEOUT)
            ),
            try
                Result = emqx_utils:nolink_apply(
                    fun() ->
                        emqx_plugin_api:dispatch(
                            CallbackModule, Method, PathRemainder, ReqInfo, Context
                        )
                    end,
                    Timeout
                ),
                map_callback_result(Result)
            catch
                exit:{timeout, _} ->
                    {503, #{code => <<"PLUGIN_API_TIMEOUT">>, message => <<"Plugin API Timeout">>}};
                Class:Reason:Stacktrace ->
                    ?SLOG(error, #{
                        msg => "plugin_api_endpoint_callback_crash",
                        callback_module => CallbackModule,
                        class => Class,
                        reason => Reason,
                        stacktrace => Stacktrace
                    }),
                    {500, #{
                        code => <<"INTERNAL_ERROR">>, message => <<"Plugin API Callback Crash">>
                    }}
            end;
        {error, _} ->
            {404, #{code => <<"NOT_FOUND">>, message => <<"Plugin API Not Found">>}}
    end.

map_callback_result({ok, Status, Headers, Body}) when is_integer(Status) ->
    {Status, normalize_headers(Headers), Body};
map_callback_result({error, Code, Msg}) ->
    {400, #{code => to_bin(Code), message => to_bin(Msg)}};
map_callback_result({error, Status, Headers, Body}) when is_integer(Status) ->
    {Status, normalize_headers(Headers), Body};
map_callback_result(_Other) ->
    {500, #{code => <<"INTERNAL_ERROR">>, message => <<"Invalid Plugin API Response">>}}.

parse_request_path(Request) ->
    %% Keep both path forms:
    %% * "/api/v5/..." when served via minirest base_path.
    %% * "/..." for direct/non-base-path routing in tests or fallback paths.
    PathSegs = binary:split(cowboy_req:path(Request), <<"/">>, [global, trim_all]),
    case PathSegs of
        [<<"api">>, <<"v5">>, <<"plugin_api">>, Plugin | PathRemainder] ->
            {Plugin, PathRemainder};
        [<<"plugin_api">>, Plugin | PathRemainder] ->
            {Plugin, PathRemainder};
        _ ->
            {undefined, []}
    end.

parse_bindings_path_remainder(Bindings) ->
    case maps:values(maps:remove(plugin, Bindings)) of
        [V | _] -> normalize_path_remainder(V);
        [] -> []
    end.

normalize_path_remainder(V) when is_binary(V) ->
    [V];
normalize_path_remainder([]) ->
    [];
normalize_path_remainder([H | T]) ->
    [to_bin(H) | [to_bin(X) || X <- T]];
normalize_path_remainder(V) ->
    [to_bin(V)].

normalize_headers(Headers) when is_map(Headers) ->
    maps:from_list([{to_bin(K), iolist_to_binary(V)} || {K, V} <- maps:to_list(Headers)]);
normalize_headers(Headers) when is_list(Headers) ->
    maps:from_list([{to_bin(K), iolist_to_binary(V)} || {K, V} <- Headers]);
normalize_headers(_) ->
    #{}.

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(S) when is_list(S) -> unicode:characters_to_binary(S);
to_bin(B) when is_binary(B) -> B.
