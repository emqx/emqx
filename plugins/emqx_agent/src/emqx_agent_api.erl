%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% REST API for the agent subsystem.
%%
%% Resources:
%%   /agent/skills                   — list / create skill instances
%%   /agent/skills/:type/:id         — get / delete a skill instance
%%   /agent/pipelines                — list / create pipeline definitions
%%   /agent/pipelines/:id            — get / update / delete a pipeline
%%
%% Skill types accepted on POST:
%%   message.publish  — MQTT publish capability scoped to a topic prefix
%%   message.request  — MQTT request/reply capability scoped to a topic prefix
%%   http             — HTTP call capability
%%   postgresql.query — PostgreSQL query
%%
%% For GET/DELETE, use the actual registry type in the :type URL segment
%% (message.publish, message.request, http, postgresql.query).

-module(emqx_agent_api).

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
dispatch(get, [<<"skills">>], Params) ->
    '/agent/skills'(get, Params);
dispatch(post, [<<"skills">>], Params) ->
    '/agent/skills'(post, Params);
dispatch(get, [<<"skills">>, Type, Id], Params) ->
    '/agent/skills/:type/:id'(get, Params#{bindings => #{type => Type, id => Id}});
dispatch(put, [<<"skills">>, Type, Id], Params) ->
    '/agent/skills/:type/:id'(put, Params#{bindings => #{type => Type, id => Id}});
dispatch(delete, [<<"skills">>, Type, Id], Params) ->
    '/agent/skills/:type/:id'(delete, Params#{bindings => #{type => Type, id => Id}});
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
            {200, #{<<"content-type">> => CT}, Data};
        {error, _} ->
            {404, #{}, <<"not found">>}
    end.

serve_html(Filename) ->
    PrivDir = code:priv_dir(emqx_agent),
    HtmlFile = filename:join(PrivDir, Filename),
    case file:read_file(HtmlFile) of
        {ok, Html} ->
            {200, #{<<"content-type">> => <<"text/html; charset=utf-8">>}, Html};
        {error, Reason} ->
            ?INTERNAL_ERROR(iolist_to_binary(io_lib:format("Cannot read UI: ~p", [Reason])))
    end.

%%--------------------------------------------------------------------
%% Handlers — Skills
%%--------------------------------------------------------------------

'/agent/skills'(get, _Params) ->
    ?OK(emqx_agent_service:skill_list());
'/agent/skills'(post, #{body := Body}) ->
    case emqx_agent_service:skill_create(Body) of
        ok ->
            ?CREATED(#{});
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(iolist_to_binary(["Missing required field: ", field_to_str(Field)]));
        {error, unknown_type} ->
            ?BAD_REQUEST(
                <<"Unknown skill type. Valid types: message.publish, message.request, http, postgresql.query">>
            );
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end.

'/agent/skills/:type/:id'(get, #{bindings := #{type := Type, id := Id}}) ->
    case emqx_agent_service:skill_get(Type, Id) of
        {ok, Skill} -> ?OK(Skill);
        {error, not_found} -> ?NOT_FOUND(<<"Skill not found">>)
    end;
'/agent/skills/:type/:id'(put, #{bindings := #{type := Type, id := Id}, body := Body}) ->
    case emqx_agent_service:skill_update(Type, Id, Body) of
        {ok, Skill} ->
            ?OK(Skill);
        {error, {missing_field, Field}} ->
            ?BAD_REQUEST(iolist_to_binary(["Missing required field: ", field_to_str(Field)]));
        {error, not_found} ->
            ?NOT_FOUND(<<"Skill not found">>);
        {error, Reason} ->
            ?BAD_REQUEST(iolist_to_binary(io_lib:format("~p", [Reason])))
    end;
'/agent/skills/:type/:id'(delete, #{bindings := #{type := Type, id := Id}}) ->
    case emqx_agent_service:skill_delete(Type, Id) of
        ok ->
            ?NO_CONTENT;
        {error, not_found} ->
            ?NOT_FOUND(<<"Skill not found">>);
        {error, {in_use, Ids}} ->
            Joined = iolist_to_binary(lists:join(<<", ">>, Ids)),
            ?CONFLICT(<<"Skill is used in pipeline(s): ", Joined/binary>>)
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
