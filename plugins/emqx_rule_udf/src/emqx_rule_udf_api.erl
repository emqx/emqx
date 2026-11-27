%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_rule_udf_api).

-moduledoc """
Plugin API, under `/api/v5/plugin_api/emqx_rule_udf/`:

- `GET ui`: the program editor page.
- `GET server`: the state of the function server (`GET /v2/info`).
- `GET programs`: all programs, without the source.
- `GET programs/{name}`: one program, with the source.
- `PUT programs/{name}`: create or replace a program. Body: `{"source", "runtime"}`.
- `DELETE programs/{name}`: delete a program.
- `POST programs/{name}/call`: run a program once. Body: `{"args": [...]}`.

Every method but `GET` changes or runs code, so it needs the global
administrator. The dashboard lets viewers send `GET` to plugin APIs.
""".

-export([handle/4]).

-include("emqx_rule_udf.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(ROLE_ADMIN, <<"administrator">>).

handle(get, Path, Request, _Context) ->
    handle_get(Path, Request);
handle(Method, Path, Request, Context) ->
    case is_global_admin(Context) of
        true ->
            handle_change(Method, Path, Request, Context);
        false ->
            error_json(
                403,
                <<"FORBIDDEN">>,
                <<"only the global administrator can change or run programs">>
            )
    end.

handle_get([<<"ui">>], _Request) ->
    {ok, 200,
        #{
            <<"content-type">> => <<"text/html; charset=utf-8">>,
            <<"cache-control">> => <<"no-store">>
        },
        ui_html()};
handle_get([<<"server">>], _Request) ->
    case emqx_rule_udf_client:server_info() of
        {ok, Info} -> ok_json(#{reachable => true, info => Info});
        {error, Reason} -> ok_json(#{reachable => false, error => format_reason(Reason)})
    end;
handle_get([<<"programs">>], _Request) ->
    ok_json(#{programs => [summary(P) || P <- emqx_rule_udf_store:list()]});
handle_get([<<"programs">>, Name], _Request) ->
    case emqx_rule_udf_store:lookup(Name) of
        {ok, P} -> ok_json(with_source(P));
        {error, not_found} -> not_found(Name)
    end;
handle_get(_Path, _Request) ->
    {error, not_found}.

handle_change(put, [<<"programs">>, Name], Request, Context) ->
    put_program(Name, Request, Context);
handle_change(delete, [<<"programs">>, Name], _Request, _Context) ->
    delete_program(Name);
handle_change(post, [<<"programs">>, Name, <<"call">>], Request, _Context) ->
    call_program(Name, Request);
handle_change(_Method, _Path, _Request, _Context) ->
    {error, not_found}.

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

put_program(Name, Request, Context) ->
    Body = maps:get(body, Request, #{}),
    maybe
        ok ?= check_name(Name),
        {ok, Source} ?= get_source(Body),
        {ok, Runtime} ?= get_runtime(Body),
        %% Save only what the function server accepts.
        {ok, _Sha256} ?= validate(Runtime, Source),
        {ok, Program} = emqx_rule_udf_store:put(Name, Runtime, Source, actor_name(Context)),
        ok_json(with_source(Program))
    end.

delete_program(Name) ->
    case emqx_rule_udf_store:delete(Name) of
        ok ->
            ok_json(#{deleted => Name});
        {error, not_found} ->
            not_found(Name)
    end.

call_program(Name, Request) ->
    case maps:get(body, Request, #{}) of
        #{<<"args">> := Args} when is_list(Args) ->
            case emqx_rule_udf_client:try_call(Name, Args) of
                {ok, Result} -> ok_json(#{result => Result});
                {error, #{reason := program_not_found}} -> not_found(Name);
                {error, Reason} -> server_error(Reason)
            end;
        _ ->
            error_json(400, <<"BAD_REQUEST">>, <<"""
            body must be {"args": [...]}
            """>>)
    end.

%%--------------------------------------------------------------------
%% Validation
%%--------------------------------------------------------------------

check_name(Name) ->
    case emqx_rule_udf_store:validate_name(Name) of
        ok -> ok;
        {error, Msg} -> error_json(400, <<"BAD_NAME">>, Msg)
    end.

get_source(#{<<"source">> := Source}) when is_binary(Source) ->
    case byte_size(Source) =< ?MAX_SOURCE_BYTES of
        true -> {ok, Source};
        false -> error_json(413, <<"TOO_LARGE">>, <<"source is larger than 64 KiB">>)
    end;
get_source(_) ->
    error_json(400, <<"BAD_REQUEST">>, <<"'source' must be a string">>).

%% The function server decides which runtimes exist (`GET /v2/info`).
get_runtime(#{<<"runtime">> := Runtime}) when is_binary(Runtime), Runtime =/= <<>> ->
    {ok, Runtime};
get_runtime(_) ->
    error_json(400, <<"BAD_REQUEST">>, <<"'runtime' must be a non-empty string">>).

validate(Runtime, Source) ->
    case emqx_rule_udf_client:validate(Runtime, Source) of
        {ok, _} = Ok -> Ok;
        {error, Reason} -> server_error(Reason)
    end.

%%--------------------------------------------------------------------
%% Authorization
%%--------------------------------------------------------------------

is_global_admin(#{auth_meta := #{actor := #{role := ?ROLE_ADMIN, namespace := ?global_ns}}}) ->
    true;
is_global_admin(_) ->
    false.

actor_name(#{auth_meta := #{source := Source}}) when is_binary(Source) -> Source;
actor_name(_) -> <<"unknown">>.

%%--------------------------------------------------------------------
%% Responses
%%--------------------------------------------------------------------

summary(#?PROGRAM_TAB{} = P) ->
    #{
        name => P#?PROGRAM_TAB.name,
        runtime => P#?PROGRAM_TAB.runtime,
        sha256 => P#?PROGRAM_TAB.sha256,
        size => byte_size(P#?PROGRAM_TAB.source),
        updated_at_ms => P#?PROGRAM_TAB.updated_at_ms,
        updated_by => P#?PROGRAM_TAB.updated_by
    }.

with_source(#?PROGRAM_TAB{source = Source} = P) ->
    (summary(P))#{source => Source}.

%% Errors from the function server keep their emqx-rule-udf/2 code. A 4xx reply,
%% except 401, keeps its status. Anything else becomes 502: a 401 from this
%% API would look like an expired dashboard session.
server_error(#{status := Status, code := Code, message := Msg} = E) ->
    HttpStatus =
        case Status of
            401 -> 502;
            S when S >= 400, S < 500 -> S;
            _ -> 502
        end,
    Detail = maps:get(detail, E, #{}),
    {error, HttpStatus, json_headers(), Detail#{code => Code, message => Msg}};
server_error(Reason) ->
    error_json(502, <<"SERVER_ERROR">>, format_reason(Reason)).

not_found(Name) ->
    error_json(404, <<"PROGRAM_NOT_FOUND">>, <<"no program named '", Name/binary, "'">>).

ok_json(Body) ->
    {ok, 200, json_headers(), Body}.

error_json(Status, Code, Msg) ->
    {error, Status, json_headers(), #{code => Code, message => Msg}}.

json_headers() ->
    #{<<"content-type">> => <<"application/json">>}.

format_reason(#{code := Code, message := Msg}) ->
    <<Code/binary, ": ", Msg/binary>>;
format_reason(Reason) ->
    iolist_to_binary(io_lib:format("~0p", [Reason])).

ui_html() ->
    {ok, Html} = file:read_file(filename:join(code:priv_dir(emqx_rule_udf), "ui.html")),
    Html.
