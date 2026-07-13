%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_http).

-moduledoc """
HTTP tool tool backed by hackney.

Invoke topic:  cap/http/<id>/request/<req_id>
Reply  topic:  cap/http/<id>/response/<req_id>

Context keys:
  <<"id">>           => binary()         — unique instance identifier
  <<"desc">>         => binary()         — human-readable description
  <<"method">>       => atom() | binary() — http method: get | post | put | patch | delete
  <<"url">>          => binary()         — URL prefix allowed for invocations
  <<"headers">>      => map() | [{binary(), binary()}]  — request headers
  <<"input_schema">> => map()            — JSON Schema for query_args (GET) or body (others)
  <<"payload_type">> => <<"json">> | <<"binary">> — response body type

Invocations use args shaped as #{path => ..., query_args => ..., body => ...}.
GET does not accept body. The invocation path is the actual HTTP path and must
remain under the configured URL path prefix.

Lifecycle:
  init()        — register the tool type
  create(Ctx)   — build a runtime tool instance; id taken from Ctx
  destroy(Tool) — clean up runtime resources owned by the tool
  deinit()      — unregister the tool type
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"http">>).

-define(QUERY_ARGS_SCHEMA, #{
    <<"type">> => <<"object">>,
    <<"properties">> => #{},
    <<"additionalProperties">> => true
}).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

%% Exported for testing
-export([append_query/2]).

%%--------------------------------------------------------------------
%% Public API
%%--------------------------------------------------------------------

-spec init() -> ok.
init() ->
    emqx_agent_tool_registry:register_type(?TOOL_TYPE, ?MODULE).

-spec deinit() -> ok.
deinit() ->
    emqx_agent_tool_registry:unregister_type(?TOOL_TYPE).

-spec create(Context :: map()) -> {ok, map()} | {error, term()}.
create(
    #{
        <<"id">> := ToolId,
        <<"desc">> := Desc,
        <<"method">> := Method,
        <<"url">> := Url,
        <<"input_schema">> := InputSchema0
    } = Context
) ->
    case decode_schema(InputSchema0) of
        {ok, InputSchema} ->
            Method1 = normalize_method(Method),
            {ok, #{
                tool_id => ToolId,
                type => ?TOOL_TYPE,
                module => ?MODULE,
                display_name => <<"HTTP Tool">>,
                description => attachment_aware_desc(Desc, Url),
                context => maps:merge(
                    #{
                        <<"payload_type">> => <<"json">>,
                        <<"autodiscover_images">> => true,
                        <<"images">> => []
                    },
                    Context#{<<"input_schema">> => InputSchema}
                ),
                input_schema => invocation_schema(Method1, Url, InputSchema)
            }};
        {error, Reason} ->
            {error, {invalid_input_schema, Reason}}
    end.

-spec destroy(map()) -> ok.
destroy(_Tool) ->
    ok.

-spec to_map(map()) -> map().
to_map(#{
    tool_id := Id,
    description := Desc,
    context := #{
        <<"method">> := Method,
        <<"url">> := Url,
        <<"headers">> := Headers,
        <<"payload_type">> := PayloadType,
        <<"autodiscover_images">> := AutodiscoverImages,
        <<"images">> := Images
    },
    input_schema := InSchema
}) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => ?TOOL_TYPE,
        <<"description">> => Desc,
        <<"method">> => Method,
        <<"url">> => Url,
        <<"headers">> => Headers,
        <<"payload_type">> => PayloadType,
        <<"autodiscover_images">> => AutodiscoverImages,
        <<"images">> => Images,
        <<"input_schema">> => InSchema
    }.

-spec handle_invoke(map(), map()) -> {ok, term()} | {ok, term(), [map()]} | {error, term()}.
handle_invoke(Context, Request) ->
    do_reply(Context, Request).

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

do_reply(Context, Request) ->
    #{<<"method">> := Method, <<"url">> := BaseUrl} = Context,
    Headers = normalize_headers(maps:get(<<"headers">>, Context, [])),
    Args = maps:get(<<"args">>, Request, #{}),

    case call(normalize_method(Method), BaseUrl, Headers, Args) of
        {ok, StatusCode, RespHeaders, RespBody} ->
            case decode_body(RespBody, Context) of
                {ok, Body} ->
                    {ok, Payload, Attachments} = emqx_agent_tool_attachments:process(
                        Body,
                        attachment_opts(Context, response_content_type(RespHeaders))
                    ),
                    Result = #{
                        <<"body">> => Payload,
                        <<"status_code">> => StatusCode,
                        <<"headers">> => response_headers_to_map(RespHeaders)
                    },
                    case Attachments of
                        [] -> {ok, Result};
                        [_ | _] -> {ok, Result, Attachments}
                    end;
                {error, Reason} ->
                    {error, emqx_agent_tool_helpers:format_error(Reason)}
            end;
        {error, Reason} ->
            {error, emqx_agent_tool_helpers:format_error({request_failed, Reason})}
    end.

%% GET — append query_args as query string; no body.
call(get, BaseUrl, Headers, Args) ->
    case invocation_url(BaseUrl, Args) of
        {ok, Url0} ->
            Url = append_query(Url0, maps:get(<<"query_args">>, Args, #{})),
            hackney:request(get, Url, Headers, <<>>, [with_body]);
        {error, _} = Error ->
            Error
    end;
%% All other methods — append query_args and send body as JSON body.
call(Method, BaseUrl, Headers, Args) ->
    case invocation_url(BaseUrl, Args) of
        {ok, Url0} ->
            Url = append_query(Url0, maps:get(<<"query_args">>, Args, #{})),
            ReqBody = emqx_utils_json:encode(maps:get(<<"body">>, Args, #{})),
            AllHeaders = [{<<"content-type">>, <<"application/json">>} | Headers],
            hackney:request(
                Method, Url, AllHeaders, ReqBody, [with_body]
            );
        {error, _} = Error ->
            Error
    end.

-spec append_query(binary(), map()) -> binary().
append_query(BaseUrl, Args) when map_size(Args) =:= 0 ->
    BaseUrl;
append_query(BaseUrl, Args) ->
    Pairs = [{to_str(K), to_str(V)} || {K, V} <- maps:to_list(Args)],
    QS = list_to_binary(uri_string:compose_query(Pairs)),
    <<BaseUrl/binary, "?", QS/binary>>.

normalize_method(<<"get">>) -> get;
normalize_method(<<"post">>) -> post;
normalize_method(<<"put">>) -> put;
normalize_method(<<"patch">>) -> patch;
normalize_method(<<"delete">>) -> delete.

normalize_headers(Headers) when is_map(Headers) -> maps:to_list(Headers);
normalize_headers(Headers) when is_list(Headers) ->
    [normalize_header(H) || H <- Headers].

normalize_header({Name, Value}) ->
    {Name, Value};
normalize_header(#{<<"name">> := Name, <<"value">> := Value}) ->
    {Name, Value};
normalize_header(#{name := Name, value := Value}) ->
    {Name, Value}.

attachment_aware_desc(Desc, Url) ->
    <<Desc/binary, " Configured URL: ", Url/binary, ".",
        " If the response contains images, the result payload uses Image <id> "
        "placeholders and the images are provided in the tool response.">>.

invocation_schema(get, Url, QueryArgsSchema) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"path">> => path_schema(Url),
            <<"query_args">> => QueryArgsSchema
        },
        <<"required">> => [<<"path">>, <<"query_args">>],
        <<"additionalProperties">> => false
    };
invocation_schema(_Method, Url, BodySchema) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"path">> => path_schema(Url),
            <<"query_args">> => ?QUERY_ARGS_SCHEMA,
            <<"body">> => BodySchema
        },
        <<"required">> => [<<"path">>, <<"query_args">>, <<"body">>],
        <<"additionalProperties">> => false
    }.

path_schema(Url) ->
    Prefix = configured_path_prefix(Url),
    #{
        <<"type">> => <<"string">>,
        <<"description">> =>
            <<
                "Actual HTTP request path. It is absolute and must start with the configured URL path prefix '",
                Prefix/binary,
                "'."
            >>
    }.

invocation_url(BaseUrl, Args) ->
    Path0 = maps:get(<<"path">>, Args, undefined),
    Path = normalize_path(Path0),
    Prefix = configured_path_prefix(BaseUrl),
    case is_path_prefix(Prefix, Path) of
        true -> {ok, <<(origin(BaseUrl))/binary, Path/binary>>};
        false -> {error, {path_outside_configured_prefix, #{prefix => Prefix, path => Path}}}
    end.

normalize_path(<<"/", _/binary>> = Path) ->
    Path;
normalize_path(<<>>) ->
    <<"/">>;
normalize_path(Path) when is_binary(Path) ->
    <<"/", Path/binary>>;
normalize_path(_) ->
    <<"/">>.

is_path_prefix(Prefix, Path) ->
    case binary:match(Path, Prefix) of
        {0, _} -> true;
        _ -> false
    end.

configured_path_prefix(Url) ->
    normalize_path(maps:get(path, uri_string:parse(Url), <<"/">>)).

origin(Url) ->
    Parts = uri_string:parse(Url),
    Scheme = maps:get(scheme, Parts),
    Host = maps:get(host, Parts),
    PortPart =
        case maps:get(port, Parts, undefined) of
            undefined -> <<>>;
            Port -> <<":", (integer_to_binary(Port))/binary>>
        end,
    <<Scheme/binary, "://", Host/binary, PortPart/binary>>.

decode_body(Body, #{<<"payload_type">> := <<"json">>}) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, Data} -> {ok, Data};
        {error, Reason} -> {error, {invalid_json_response, Reason}}
    end;
decode_body(Body, #{<<"payload_type">> := <<"binary">>}) ->
    {ok, Body}.

attachment_opts(
    #{
        <<"autodiscover_images">> := AutodiscoverImages,
        <<"images">> := Images
    },
    ContentType
) ->
    #{
        autodiscover_images => AutodiscoverImages,
        images => Images,
        content_type => ContentType
    }.

response_content_type(Headers) ->
    maps:get(<<"content-type">>, response_headers_to_map(Headers), undefined).

response_headers_to_map(Headers) ->
    maps:from_list([{string:lowercase(Name), Value} || {Name, Value} <- Headers]).

to_str(V) when is_binary(V) -> binary_to_list(V);
to_str(V) when is_integer(V) -> integer_to_list(V);
to_str(V) when is_float(V) -> float_to_list(V, [{decimals, 10}, compact]);
to_str(V) when is_atom(V) -> atom_to_list(V).

decode_schema(V) when is_binary(V) ->
    try emqx_utils_json:decode(V) of
        Decoded -> {ok, Decoded}
    catch
        _:Reason -> {error, {invalid_json, Reason}}
    end;
decode_schema(V) ->
    {ok, V}.
