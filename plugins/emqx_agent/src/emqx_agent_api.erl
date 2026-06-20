%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_api).

-moduledoc """
REST API for the agent subsystem.

Resources:
  /agent/ui                       — serve the main agent UI
  /agent/apple-box/ui             — serve the apple-box demo UI
  /agent/apple-box/img/:file      — serve apple-box demo images
  /agent/builder/ui               — serve the builder UI
  /agent/assets/:file             — serve bundled UI assets
  /agent/ui/assets/:file          — serve bundled UI assets
  /agent/providers                — list configured AI providers
  /agent/tools                   — list / create configured tools
  /agent/tools/statuses          — runtime tool reconciliation statuses
  /agent/tools/:type/:id         — get / update / delete a configured tool
  /agent/connections              — list / create tool connections
  /agent/connections/statuses     — runtime connection reconciliation statuses
  /agent/connections/:id          — get / update / delete a tool connection
  /agent/connections/:id/start    — enable and reconcile a tool connection
  /agent/connections/:id/stop     — disable and reconcile a tool connection
  /agent/pipelines                — list / create pipeline definitions
  /agent/pipelines/:id            — get / update / delete a pipeline

Tool types accepted on POST:
  message__publish  — MQTT publish capability scoped to a topic prefix
  message__request  — MQTT request/reply capability scoped to a topic prefix
  http             — HTTP call capability
  postgresql__query — PostgreSQL query
  stream_write      — Write data to an EMQX stream
  stream_read       — Read data from an EMQX stream
  stream_del        — Delete data from an EMQX stream
  kv_write          — Write a value to a last-value stream key
  kv_read           — Read a value from a last-value stream key
  kv_read_all       — Read all values from a last-value stream
  kv_del            — Delete a key from a last-value stream
  kv_clear          — Clear a last-value stream

For GET/DELETE, use the actual registry type in the :type URL segment
(message__publish, message__request, http, postgresql__query, stream_write, stream_read, stream_del, kv_write, kv_read, kv_read_all, kv_del, kv_clear).
""".

-include_lib("emqx_utils/include/emqx_http_api.hrl").

-export([
    handle/3
]).

%%--------------------------------------------------------------------
%% Plugin API gateway callback
%%--------------------------------------------------------------------

handle(Method, Path0, Request) ->
    Path = normalize_plugin_path(Path0),
    Params = #{body => maps:get(body, Request, #{})},
    case dispatch(Method, Path, Params) of
        {error, not_found} ->
            {error, not_found};
        Response ->
            normalize_plugin_response(Response)
    end.

dispatch(get, [<<"ui">>], Params) ->
    '/agent/ui'(get, Params);
dispatch(get, [<<"apple-box">>, <<"ui">>], Params) ->
    '/agent/apple-box/ui'(get, Params);
dispatch(get, [<<"apple-box">>, <<"img">>, File], Params) ->
    '/agent/apple-box/img/:file'(get, Params#{bindings => #{file => File}});
dispatch(get, [<<"builder">>, <<"ui">>], Params) ->
    '/agent/builder/ui'(get, Params);
dispatch(get, [<<"assets">>, File], _Params) ->
    serve_ui_asset(File);
dispatch(get, [<<"ui">>, <<"assets">>, File], _Params) ->
    serve_ui_asset(File);
dispatch(get, [<<"tools">>], Params) ->
    '/agent/tools'(get, Params);
dispatch(post, [<<"tools">>], Params) ->
    '/agent/tools'(post, Params);
dispatch(get, [<<"tools">>, <<"statuses">>], Params) ->
    '/agent/tools/statuses'(get, Params);
dispatch(get, [<<"tools">>, Type, Id], Params) ->
    '/agent/tools/:type/:id'(get, Params#{bindings => #{type => Type, id => Id}});
dispatch(put, [<<"tools">>, Type, Id], Params) ->
    '/agent/tools/:type/:id'(put, Params#{bindings => #{type => Type, id => Id}});
dispatch(delete, [<<"tools">>, Type, Id], Params) ->
    '/agent/tools/:type/:id'(delete, Params#{bindings => #{type => Type, id => Id}});
dispatch(get, [<<"connections">>], Params) ->
    '/agent/connections'(get, Params);
dispatch(post, [<<"connections">>], Params) ->
    '/agent/connections'(post, Params);
dispatch(get, [<<"connections">>, <<"statuses">>], Params) ->
    '/agent/connections/statuses'(get, Params);
dispatch(get, [<<"connections">>, Id], Params) ->
    '/agent/connections/:id'(get, Params#{bindings => #{id => Id}});
dispatch(put, [<<"connections">>, Id], Params) ->
    '/agent/connections/:id'(put, Params#{bindings => #{id => Id}});
dispatch(delete, [<<"connections">>, Id], Params) ->
    '/agent/connections/:id'(delete, Params#{bindings => #{id => Id}});
dispatch(post, [<<"connections">>, Id, <<"start">>], Params) ->
    '/agent/connections/:id/start'(post, Params#{bindings => #{id => Id}});
dispatch(post, [<<"connections">>, Id, <<"stop">>], Params) ->
    '/agent/connections/:id/stop'(post, Params#{bindings => #{id => Id}});
dispatch(get, [<<"providers">>], Params) ->
    '/agent/providers'(get, Params);
dispatch(get, [<<"pipelines">>], Params) ->
    '/agent/pipelines'(get, Params);
dispatch(post, [<<"pipelines">>], Params) ->
    '/agent/pipelines'(post, Params);
dispatch(get, [<<"pipelines">>, Id], Params) ->
    '/agent/pipelines/:id'(get, Params#{bindings => #{id => Id}});
dispatch(put, [<<"pipelines">>, Id], Params) ->
    '/agent/pipelines/:id'(put, Params#{bindings => #{id => Id}});
dispatch(delete, [<<"pipelines">>, Id], Params) ->
    '/agent/pipelines/:id'(delete, Params#{bindings => #{id => Id}});
dispatch(_Method, _Path, _Params) ->
    {error, not_found}.

normalize_plugin_path([<<"agent">> | Rest]) ->
    Rest;
normalize_plugin_path(Path) ->
    Path.

normalize_plugin_response({Status, Headers, Body}) when is_integer(Status) ->
    {ok, Status, Headers, Body};
normalize_plugin_response({Status, Body}) when is_integer(Status) ->
    {ok, Status, #{}, Body};
normalize_plugin_response({Status}) when is_integer(Status) ->
    {ok, Status, #{}, <<>>};
normalize_plugin_response(Status) when is_integer(Status) ->
    {ok, Status, #{}, <<>>}.

%%--------------------------------------------------------------------
%% Handler — AI Providers
%%--------------------------------------------------------------------

'/agent/providers'(get, _Params) ->
    Providers = [emqx_utils:redact(P) || P <- emqx_ai_completion_config:get_providers_raw()],
    ?OK(Providers).

%%--------------------------------------------------------------------
%% Handler — UI
%%--------------------------------------------------------------------

'/agent/ui'(get, _Params) ->
    serve_html("index.html").

'/agent/apple-box/ui'(get, _Params) ->
    serve_html("apple-box.html").

'/agent/builder/ui'(get, _Params) ->
    serve_html("builder.html").

'/agent/apple-box/img/:file'(get, #{bindings := #{file := File}}) ->
    PrivDir = code:priv_dir(emqx_agent),
    ImgFile = filename:join([PrivDir, "img", File]),
    case file:read_file(ImgFile) of
        {ok, Data} ->
            CT =
                case filename:extension(File) of
                    <<".png">> -> <<"image/png">>;
                    <<".jpg">> -> <<"image/jpeg">>;
                    <<".jpeg">> -> <<"image/jpeg">>;
                    _ -> <<"application/octet-stream">>
                end,
            {200, no_cache_headers(CT), Data};
        {error, _} ->
            {404, #{}, <<"not found">>}
    end.

serve_html(Filename) ->
    PrivDir = code:priv_dir(emqx_agent),
    HtmlFile = filename:join(PrivDir, Filename),
    case file:read_file(HtmlFile) of
        {ok, Html} ->
            {200, no_cache_headers(<<"text/html; charset=utf-8">>), Html};
        {error, Reason} ->
            ?INTERNAL_ERROR(iolist_to_binary(io_lib:format("Cannot read UI: ~p", [Reason])))
    end.

serve_ui_asset(File) ->
    case is_safe_basename(File) of
        false ->
            {404, #{}, <<"not found">>};
        true ->
            PrivDir = code:priv_dir(emqx_agent),
            AssetFile = filename:join([PrivDir, <<"ui">>, File]),
            case file:read_file(AssetFile) of
                {ok, Data} ->
                    CT = ui_asset_content_type(filename:extension(File)),
                    {200, no_cache_headers(CT), Data};
                {error, _} ->
                    {404, #{}, <<"not found">>}
            end
    end.

is_safe_basename(File) ->
    case re:run(File, <<"[\\\\/]|\\.\\.">>) of
        nomatch -> true;
        _ -> false
    end.

ui_asset_content_type(<<".css">>) -> <<"text/css; charset=utf-8">>;
ui_asset_content_type(<<".js">>) -> <<"application/javascript; charset=utf-8">>;
ui_asset_content_type(<<".html">>) -> <<"text/html; charset=utf-8">>;
ui_asset_content_type(<<".json">>) -> <<"application/json; charset=utf-8">>;
ui_asset_content_type(<<".png">>) -> <<"image/png">>;
ui_asset_content_type(<<".jpg">>) -> <<"image/jpeg">>;
ui_asset_content_type(<<".jpeg">>) -> <<"image/jpeg">>;
ui_asset_content_type(<<".svg">>) -> <<"image/svg+xml">>;
ui_asset_content_type(_) -> <<"application/octet-stream">>.

no_cache_headers(ContentType) ->
    #{
        <<"content-type">> => ContentType,
        <<"cache-control">> => <<"no-store">>
    }.

%%--------------------------------------------------------------------
%% Handlers — Tools
%%--------------------------------------------------------------------

'/agent/tools'(get, _Params) ->
    ?OK(emqx_agent_service:tool_list());
'/agent/tools'(post, #{body := Body}) ->
    case emqx_agent_service:tool_create(Body) of
        ok ->
            ?CREATED(#{});
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(iolist_to_binary(["Missing required field: ", field_to_str(Field)]));
        {error, already_exists} ->
            ?CONFLICT(<<"Tool already exists">>);
        {error, unknown_type} ->
            ?BAD_REQUEST(
                <<"Unknown tool type. Valid types: message__publish, message__request, http, postgresql__query, stream_write, stream_read, stream_del, kv_write, kv_read, kv_read_all, kv_del, kv_clear">>
            );
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

'/agent/tools/statuses'(get, _Params) ->
    ?OK(emqx_agent_service:tool_statuses()).

'/agent/tools/:type/:id'(get, #{bindings := #{type := Type, id := Id}}) ->
    case emqx_agent_service:tool_get(Type, Id) of
        {ok, Tool} -> ?OK(Tool);
        {error, not_found} -> ?NOT_FOUND(<<"Tool not found">>)
    end;
'/agent/tools/:type/:id'(put, #{bindings := #{type := Type, id := Id}, body := Body}) ->
    case emqx_agent_service:tool_update(Type, Id, Body) of
        {ok, Tool} ->
            ?OK(Tool);
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(iolist_to_binary(["Missing required field: ", field_to_str(Field)]));
        {error, not_found} ->
            ?NOT_FOUND(<<"Tool not found">>);
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end;
'/agent/tools/:type/:id'(delete, #{bindings := #{type := Type, id := Id}}) ->
    case emqx_agent_service:tool_delete(Type, Id) of
        ok ->
            ?NO_CONTENT;
        {error, not_found} ->
            ?NOT_FOUND(<<"Tool not found">>);
        {error, {in_use, Ids}} ->
            Joined = iolist_to_binary(lists:join(<<", ">>, Ids)),
            ?CONFLICT(<<"Tool is used in pipeline(s): ", Joined/binary>>)
    end.

%%--------------------------------------------------------------------
%% Handlers — Connections
%%--------------------------------------------------------------------

'/agent/connections'(get, _Params) ->
    ?OK(emqx_agent_service:connection_list());
'/agent/connections'(post, #{body := Body}) ->
    case emqx_agent_service:connection_create(Body) of
        ok ->
            ?CREATED(#{});
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(iolist_to_binary(["Missing required field: ", field_to_str(Field)]));
        {error, already_exists} ->
            ?CONFLICT(<<"Connection already exists">>);
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

'/agent/connections/statuses'(get, _Params) ->
    ?OK(emqx_agent_service:connection_statuses()).

'/agent/connections/:id'(get, #{bindings := #{id := Id}}) ->
    case emqx_agent_service:connection_get(Id) of
        {ok, Connection} -> ?OK(Connection);
        {error, not_found} -> ?NOT_FOUND(<<"Connection not found">>)
    end;
'/agent/connections/:id'(put, #{bindings := #{id := Id}, body := Body}) ->
    case emqx_agent_service:connection_update(Id, Body) of
        {ok, Connection} ->
            ?OK(Connection);
        {error, not_found} ->
            ?NOT_FOUND(<<"Connection not found">>);
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end;
'/agent/connections/:id'(delete, #{bindings := #{id := Id}}) ->
    case emqx_agent_service:connection_delete(Id) of
        ok ->
            ?NO_CONTENT;
        {error, not_found} ->
            ?NOT_FOUND(<<"Connection not found">>);
        {error, {in_use, Ids}} ->
            Joined = iolist_to_binary(lists:join(<<", ">>, Ids)),
            ?CONFLICT(<<"Connection is used by tool(s): ", Joined/binary>>);
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

'/agent/connections/:id/start'(post, #{bindings := #{id := Id}}) ->
    case emqx_agent_service:connection_start(Id) of
        {ok, Connection} -> ?OK(Connection);
        {error, not_found} -> ?NOT_FOUND(<<"Connection not found">>);
        {error, Reason} -> ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

'/agent/connections/:id/stop'(post, #{bindings := #{id := Id}}) ->
    case emqx_agent_service:connection_stop(Id) of
        {ok, Connection} -> ?OK(Connection);
        {error, not_found} -> ?NOT_FOUND(<<"Connection not found">>);
        {error, Reason} -> ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

%%--------------------------------------------------------------------
%% Handlers — Pipelines
%%--------------------------------------------------------------------

'/agent/pipelines'(get, _Params) ->
    ?OK(emqx_agent_service:pipeline_list());
'/agent/pipelines'(post, #{body := Body}) ->
    case emqx_agent_service:pipeline_create(Body) of
        ok ->
            ?CREATED(#{});
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(iolist_to_binary(["Missing required field: ", field_to_str(Field)]));
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

'/agent/pipelines/:id'(get, #{bindings := #{id := Id}}) ->
    case emqx_agent_service:pipeline_get(Id) of
        {ok, Pipeline} -> ?OK(Pipeline);
        {error, not_found} -> ?NOT_FOUND(<<"Pipeline not found">>)
    end;
'/agent/pipelines/:id'(put, #{bindings := #{id := Id}, body := Body}) ->
    case emqx_agent_service:pipeline_update(Id, Body) of
        {ok, Pipeline} -> ?OK(Pipeline);
        {error, Reason} -> ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end;
'/agent/pipelines/:id'(delete, #{bindings := #{id := Id}}) ->
    case emqx_agent_service:pipeline_delete(Id) of
        ok ->
            ?NO_CONTENT;
        {error, not_found} ->
            ?NOT_FOUND(<<"Pipeline not found">>);
        {error, pipeline_is_active} ->
            ?CONFLICT(<<"Pipeline is active; set active=false before deleting">>)
    end.

field_to_str(F) when is_binary(F) -> F;
field_to_str(F) when is_atom(F) -> atom_to_binary(F, utf8).
