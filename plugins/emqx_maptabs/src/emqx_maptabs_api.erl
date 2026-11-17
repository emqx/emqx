%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_api).

-moduledoc """
REST handlers, served by the EMQX plugin API gateway under
`/api/v5/plugin_api/emqx_maptabs/`:

- `GET    tables`        tables cached on every running node
- `GET    tables/:name`  stored table content
- `PUT    tables/:name`  validate, store and load a table
- `DELETE tables/:name`  delete a table (idempotent)
- `POST   reload`        rebuild the cache from storage on every node

A write is answered with 200/204 only when every running node has
confirmed its cache. Otherwise the answer is 503 with the nodes that
did not confirm; the stored change is kept, and repeating the same
request completes it.
""".

-export([handle/3]).

%% The gateway's own default for `plugins.api_endpoint.timeout'
-define(DEFAULT_GATEWAY_TIMEOUT, 5000).

-define(VALIDATION_ERRORS, [
    invalid_table_name,
    invalid_json,
    not_a_json_array,
    row_not_an_object,
    missing_key,
    float_key,
    invalid_key_type,
    duplicate_key,
    too_many_rows,
    too_many_tables,
    table_file_too_large
]).

handle(get, [<<"tables">>], _Request) ->
    Nodes = emqx_maptabs:list_cluster(deadline()),
    {ok, 200, #{}, [node_tables(Node, Result) || {Node, Result} <- Nodes]};
handle(get, [<<"tables">>, Name], _Request) ->
    case emqx_maptabs:read_table(Name) of
        {ok, Json} ->
            %% the stored JSON is sent as is; minirest sets the JSON
            %% content type only for map and list bodies it encodes
            {ok, 200, #{<<"content-type">> => <<"application/json">>}, Json};
        {error, not_found} ->
            not_found(Name);
        {error, Reason} ->
            error_response(Reason)
    end;
handle(put, [<<"tables">>, Name], Request) ->
    Deadline = deadline(),
    maybe
        ok ?= emqx_maptabs_loader:validate_name(Name),
        {ok, Json} ?= table_json(Request),
        {ok, Info} ?= emqx_maptabs:put_table(Name, Json, Deadline),
        {ok, 200, #{}, json(Info)}
    else
        {error, Reason} -> error_response(Reason)
    end;
handle(delete, [<<"tables">>, Name], _Request) ->
    case emqx_maptabs:delete_table(Name, deadline()) of
        ok ->
            {ok, 204, #{}, <<>>};
        {error, Reason} ->
            error_response(Reason)
    end;
handle(post, [<<"reload">>], _Request) ->
    Results = emqx_maptabs:reconcile_cluster(deadline()),
    Confirmed = [Node || {Node, ok} <- Results],
    case [{Node, failure_reason(Result)} || {Node, Result} <- Results, Result =/= ok] of
        [] -> {ok, 200, #{}, json(#{nodes => Confirmed})};
        Failed -> error_response({partial, #{nodes => Confirmed}, Failed})
    end;
handle(_Method, _Path, _Request) ->
    {error, not_found}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% The gateway kills the callback when its budget runs out, and the
%% caller then learns nothing about which nodes confirmed. Finish the
%% cluster fan-out well inside the same budget.
deadline() ->
    Budget = emqx:get_config(
        [plugins, api_endpoint, timeout],
        emqx:get_config([plugins, api_gateway, timeout], ?DEFAULT_GATEWAY_TIMEOUT)
    ),
    erlang:monotonic_time(millisecond) + Budget * 3 div 4.

%% The body must be `application/json'. The gateway hands it over
%% already decoded, so the stored bytes are its canonical encoding:
%% equal content uploaded twice has one version, whatever its
%% formatting.
table_json(Request) ->
    case is_json_body(maps:get(headers, Request, #{})) of
        true -> {ok, emqx_utils_json:encode(maps:get(body, Request, #{}))};
        false -> {error, unsupported_content_type}
    end.

is_json_body(#{<<"content-type">> := ContentType}) ->
    try cow_http_hd:parse_content_type(ContentType) of
        {<<"application">>, <<"json">>, _} -> true;
        _ -> false
    catch
        _:_ -> false
    end;
is_json_body(_Headers) ->
    false.

failure_reason({error, Reason}) -> Reason;
failure_reason(Other) -> Other.

node_tables(Node, Tables) when is_list(Tables) ->
    #{node => Node, tables => json(Tables)};
node_tables(Node, {error, Reason}) ->
    #{node => Node, error => json(Reason)}.

not_found(Name) ->
    {error, 404, #{}, #{code => <<"NOT_FOUND">>, message => <<"Table not found">>, name => Name}}.

error_response(unsupported_content_type) ->
    {error, 415, #{}, #{
        code => <<"UNSUPPORTED_MEDIA_TYPE">>,
        message => <<"Content-Type must be application/json">>
    }};
error_response({partial, Info, Failed}) ->
    {error, 503, #{},
        json(Info#{
            code => <<"PARTIALLY_APPLIED">>,
            message =>
                <<"Not every node confirmed the change. It is stored and is not rolled back; ",
                    "repeat the request to complete it.">>,
            failed_nodes => [#{node => Node, reason => Reason} || {Node, Reason} <- Failed]
        })};
error_response(#{reason := Reason} = Detail) ->
    case lists:member(Reason, ?VALIDATION_ERRORS) of
        true ->
            %% `value' echoes the rejected body back
            Body = maps:without([value], Detail),
            {error, 400, #{}, json(Body#{code => <<"BAD_REQUEST">>, message => Reason})};
        false ->
            {error, 503, #{}, json(Detail#{code => <<"SERVICE_UNAVAILABLE">>, message => Reason})}
    end;
error_response(Reason) ->
    {error, 503, #{},
        json(#{
            code => <<"SERVICE_UNAVAILABLE">>,
            message => <<"Service unavailable">>,
            reason => Reason
        })}.

json(Term) ->
    emqx_maptabs_cli:json_term(Term).
