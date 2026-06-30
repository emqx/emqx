%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_tool_image_fetch).

-moduledoc """
Image URL fetch tool.

This tool validates an image URL against a configured base URL and returns it as
an attachment for the LLM. It intentionally does not perform any HTTP request.
""".

-behaviour(emqx_agent_tool).

-define(TOOL_TYPE, <<"image__fetch">>).

-export([init/0, deinit/0, create/1, destroy/1, to_map/1, handle_invoke/2]).

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
create(#{<<"id">> := ToolId, <<"desc">> := Desc, <<"url">> := Url} = Context) ->
    {ok, #{
        tool_id => ToolId,
        type => ?TOOL_TYPE,
        module => ?MODULE,
        display_name => <<"Image Fetch Tool">>,
        description => description(Desc, Url),
        context => Context,
        input_schema => input_schema(Url)
    }}.

-spec destroy(map()) -> ok.
destroy(_Tool) ->
    ok.

-spec to_map(map()) -> map().
to_map(#{
    tool_id := Id,
    description := Desc,
    context := #{<<"url">> := Url},
    input_schema := InputSchema
}) ->
    #{
        <<"tool_id">> => Id,
        <<"type">> => ?TOOL_TYPE,
        <<"description">> => Desc,
        <<"url">> => Url,
        <<"input_schema">> => InputSchema
    }.

-spec handle_invoke(map(), map()) -> {ok, map(), [map()]} | {error, binary()}.
handle_invoke(#{<<"url">> := BaseUrl}, Request) ->
    Args = maps:get(<<"args">>, Request, #{}),
    case invocation_url(BaseUrl, Args) of
        {ok, Url} ->
            {ok, #{<<"url">> => Url, <<"image">> => <<"Image .url">>}, [
                #{
                    <<"id">> => <<".url">>,
                    <<"type">> => <<"image">>,
                    <<"url">> => Url
                }
            ]};
        {error, Reason} ->
            {error, emqx_agent_tool_helpers:format_error(Reason)}
    end.

%%--------------------------------------------------------------------
%% Internal
%%--------------------------------------------------------------------

description(Desc, Url) ->
    <<Desc/binary, " Configured URL: ", Url/binary, ". ",
        "Call this tool with an image URL under the configured URL. ",
        "The tool validates the URL and returns it as an image attachment without fetching it.">>.

input_schema(Url) ->
    #{
        <<"type">> => <<"object">>,
        <<"properties">> => #{
            <<"url">> => #{
                <<"type">> => <<"string">>,
                <<"description">> => url_description(Url)
            }
        },
        <<"required">> => [<<"url">>],
        <<"additionalProperties">> => false
    }.

url_description(Url) ->
    Prefix = configured_path_prefix(Url),
    <<"Full image URL. It must be under the configured base URL '", Url/binary,
        "' (same origin and path prefix '", Prefix/binary, "').">>.

invocation_url(BaseUrl, Args) ->
    case maps:get(<<"url">>, Args, undefined) of
        Url when is_binary(Url) -> validate_url(BaseUrl, Url);
        Other -> {error, {invalid_image_url, #{url => Other}}}
    end.

validate_url(BaseUrl, Url) ->
    try
        Prefix = configured_path_prefix(BaseUrl),
        case
            {
                origin(BaseUrl),
                origin(Url),
                normalize_path(maps:get(path, uri_string:parse(Url), <<"/">>))
            }
        of
            {Origin, Origin, Path} ->
                case is_path_prefix(Prefix, Path) of
                    true ->
                        {ok, Url};
                    false ->
                        {error, {url_outside_configured_prefix, #{prefix => Prefix, url => Url}}}
                end;
            {BaseOrigin, UrlOrigin, _Path} ->
                {error,
                    {url_outside_configured_origin, #{
                        origin => BaseOrigin, url_origin => UrlOrigin
                    }}}
        end
    catch
        _:Reason -> {error, {invalid_image_url, #{url => Url, reason => Reason}}}
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
