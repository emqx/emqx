%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_plugins_api_endpoint).

-behaviour(minirest_api).
-compile(nowarn_undefined_behaviour).

-export([api_spec/0, paths/0, schema/1]).
-export([gateway/3]).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_utils/include/emqx_http_api.hrl").

-define(DEFAULT_TIMEOUT, 5000).

api_spec() ->
    emqx_dashboard_swagger:spec(?MODULE, #{check_schema => false}).

paths() ->
    ["/plugin_api/:plugin/[...]"].

schema("/plugin_api/:plugin/[...]") ->
    #{
        'operationId' => gateway,
        get => gateway_schema(get),
        post => gateway_schema(post),
        put => gateway_schema(put),
        patch => gateway_schema(patch),
        delete => gateway_schema(delete)
    }.

gateway_schema(Method) when Method =:= get; Method =:= delete ->
    gateway_schema_base();
gateway_schema(_Method) ->
    gateway_schema_base(#{
        'requestBody' => #{
            content => #{
                'application/json' => #{
                    schema => hoconsc:mk(map())
                }
            }
        }
    }).

gateway_schema_base() ->
    gateway_schema_base(#{}).

gateway_schema_base(Extra) ->
    maps:merge(
        #{
            tags => [<<"Plugins">>],
            description => ?DESC("gateway_desc"),
            parameters => [plugin_param()],
            responses => #{
                200 => hoconsc:mk(map()),
                400 => hoconsc:mk(map()),
                401 => hoconsc:mk(map()),
                404 => hoconsc:mk(map()),
                500 => hoconsc:mk(map()),
                503 => hoconsc:mk(map())
            }
        },
        Extra
    ).

plugin_param() ->
    {plugin, hoconsc:mk(binary(), #{in => path, required => true, example => <<"my_plugin">>})}.

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
        namespace => request_namespace(Params)
    },
    case Plugin of
        undefined ->
            ?NOT_FOUND(<<"Plugin API Not Found">>);
        _ ->
            call_plugin_api(Plugin, Method, PathRemainder, ReqInfo, Context)
    end.

call_plugin_api(Plugin, Method, PathRemainder, ReqInfo, Context) ->
    Timeout = emqx:get_config(
        [plugins, api_endpoint, timeout],
        emqx:get_config([plugins, api_gateway, timeout], ?DEFAULT_TIMEOUT)
    ),
    Request = #{
        method => Method,
        path => PathRemainder,
        request => ReqInfo,
        context => Context
    },
    emqx_plugins:handle_api_call(Plugin, Request, Timeout).

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

to_bin(A) when is_atom(A) -> atom_to_binary(A, utf8);
to_bin(S) when is_list(S) -> unicode:characters_to_binary(S);
to_bin(B) when is_binary(B) -> B.

request_namespace(#{auth_meta := #{namespace := Namespace}}) when is_binary(Namespace) ->
    Namespace;
request_namespace(#{auth_meta := #{namespace := ?global_ns}}) ->
    ?global_ns;
request_namespace(_Request) ->
    ?global_ns.
