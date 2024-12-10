%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_es_connector).

-behaviour(emqx_connector_examples).

-behaviour(emqx_resource).

-include("emqx_bridge_es.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_query/3,
    on_query_async/4,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3,
    on_format_query_result/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    connector_examples/1,
    connector_example_values/0
]).

-export([render_template/2]).
-export([convert_server/2]).

%% emqx_connector_resource behaviour callbacks
-export([connector_config/2]).

-type config() ::
    #{
        request_base := #{
            scheme := http | https,
            host := iolist(),
            port := inet:port_number()
        },
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        pool_size := pos_integer(),
        request => undefined | map(),
        atom() => _
    }.

-type state() ::
    #{
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        channels := map(),
        request => undefined | map(),
        atom() => _
    }.

-type manager_id() :: binary().

-define(CONNECTOR_TYPE, elasticsearch).

%%-------------------------------------------------------------------------------------
%% connector examples
%%-------------------------------------------------------------------------------------
connector_examples(Method) ->
    [
        #{
            <<"elasticsearch">> =>
                #{
                    summary => <<"Elastic Search Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_example_values()
                    )
                }
        }
    ].

connector_example_values() ->
    #{
        name => <<"elasticsearch_connector">>,
        type => elasticsearch,
        enable => true,
        authentication => #{
            <<"username">> => <<"root">>,
            <<"password">> => <<"******">>
        },
        server => <<"127.0.0.1:9200">>,
        connect_timeout => <<"15s">>,
        pool_type => <<"random">>,
        pool_size => 8,
        enable_pipelining => 100,
        ssl => #{enable => false}
    }.

%%-------------------------------------------------------------------------------------
%% schema
%%-------------------------------------------------------------------------------------
namespace() -> "elasticsearch".

roots() ->
    [{config, #{type => ?R_REF(config)}}].

fields(config) ->
    lists:filter(
        fun({K, _}) -> not lists:member(K, [url, request, retry_interval, headers]) end,
        emqx_bridge_http_schema:fields("config_connector")
    ) ++
        fields("connection_fields");
fields("connection_fields") ->
    [
        {server, server()},
        {authentication,
            ?HOCON(
                ?UNION([?R_REF(auth_basic)]),
                #{
                    desc => ?DESC("config_authentication")
                }
            )}
    ];
fields(auth_basic) ->
    [
        {username,
            ?HOCON(binary(), #{
                required => true,
                desc => ?DESC("config_auth_basic_username")
            })},
        {password,
            emqx_schema_secret:mk(#{
                required => true,
                desc => ?DESC("config_auth_basic_password")
            })}
    ];
fields("post") ->
    emqx_connector_schema:type_and_name_fields(elasticsearch) ++ fields(config);
fields("put") ->
    fields(config);
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc(config) ->
    ?DESC("desc_config");
desc(auth_basic) ->
    "Basic Authentication";
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for Elastic Search using `", string:to_upper(Method), "` method."];
desc("server") ->
    ?DESC("server");
desc(_) ->
    undefined.

server() ->
    Meta = #{
        required => true,
        default => <<"127.0.0.1:9200">>,
        desc => ?DESC("server"),
        converter => fun ?MODULE:convert_server/2
    },
    emqx_schema:servers_sc(Meta, #{default_port => 9200}).

convert_server(<<"http://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(<<"https://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(Server0, HoconOpts) ->
    Server = string:trim(Server0, trailing, "/"),
    emqx_schema:convert_servers(Server, HoconOpts).

connector_config(Conf, #{name := Name, parse_confs := ParseConfs}) ->
    WebhookConfig =
        Conf#{
            method => <<"post">>,
            url => base_url(Conf),
            headers => [
                {<<"Content-type">>, <<"application/json">>},
                {<<"Authorization">>, basic_token(Conf)}
            ]
        },
    ParseConfs(
        <<"http">>,
        Name,
        WebhookConfig
    ).

basic_token(#{
    authentication :=
        #{
            username := Username,
            password := Password0
        }
}) ->
    Password = emqx_secret:unwrap(Password0),
    Base64 = base64:encode(<<Username/binary, ":", Password/binary>>),
    <<"Basic ", Base64/binary>>.

base_url(#{ssl := #{enable := true}, server := Server}) -> "https://" ++ Server;
base_url(#{server := Server}) -> "http://" ++ Server.
%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------
resource_type() -> elastic_search.

callback_mode() -> async_if_possible.

-spec on_start(manager_id(), config()) -> {ok, state()} | no_return().
on_start(InstanceId, Config) ->
    case emqx_bridge_http_connector:on_start(InstanceId, Config) of
        {ok, State} ->
            ?SLOG(info, #{
                msg => "elasticsearch_bridge_started",
                instance_id => InstanceId,
                request => emqx_utils:redact(maps:get(request, State, <<>>))
            }),
            ?tp(elasticsearch_bridge_started, #{instance_id => InstanceId}),
            {ok, State#{channels => #{}}};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_elasticsearch_bridge",
                instance_id => InstanceId,
                request => emqx_utils:redact(maps:get(request, Config, <<>>)),
                reason => Reason
            }),
            throw(failed_to_start_elasticsearch_bridge)
    end.

-spec on_stop(manager_id(), state()) -> ok | {error, term()}.
on_stop(InstanceId, State) ->
    ?SLOG(info, #{
        msg => "stopping_elasticsearch_bridge",
        connector => InstanceId
    }),
    Res = emqx_bridge_http_connector:on_stop(InstanceId, State),
    ?tp(elasticsearch_bridge_stopped, #{instance_id => InstanceId}),
    Res.

-spec on_get_status(manager_id(), state()) ->
    ?status_connected | {?status_disconnected, term()}.
on_get_status(InstanceId, State) ->
    emqx_bridge_http_connector:on_get_status(InstanceId, State).

-spec on_query(manager_id(), tuple(), state()) ->
    {ok, pos_integer(), [term()], term()}
    | {ok, pos_integer(), [term()]}
    | {error, term()}.
on_query(InstanceId, {ChannelId, Msg} = Req, State) ->
    ?tp(elasticsearch_bridge_on_query, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "elasticsearch_bridge_on_query_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),
    handle_response(
        emqx_bridge_http_connector:on_query(
            InstanceId, {ChannelId, Msg}, State
        )
    ).

-spec on_query_async(manager_id(), tuple(), {function(), [term()]}, state()) ->
    {ok, pid()} | {error, empty_request}.
on_query_async(
    InstanceId, {ChannelId, Msg} = Req, ReplyFunAndArgs0, State
) ->
    ?tp(elasticsearch_bridge_on_query_async, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "elasticsearch_bridge_on_query_async_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),
    ReplyFunAndArgs =
        {
            fun(Result) ->
                Response = handle_response(Result),
                emqx_resource:apply_reply_fun(ReplyFunAndArgs0, Response)
            end,
            []
        },
    emqx_bridge_http_connector:on_query_async(
        InstanceId, {ChannelId, Msg}, ReplyFunAndArgs, State
    ).

on_format_query_result(Result) ->
    emqx_bridge_http_connector:on_format_query_result(Result).

on_add_channel(
    InstanceId,
    #{channels := Channels} = State0,
    ChannelId,
    #{parameters := Parameter}
) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            {error, already_exists};
        _ ->
            Parameter1 = Parameter#{
                path => path(Parameter),
                method => method(Parameter),
                body => get_body_template(Parameter)
            },
            ChannelConfig = #{
                parameters => Parameter1,
                render_template_func => fun ?MODULE:render_template/2
            },
            {ok, State} = emqx_bridge_http_connector:on_add_channel(
                InstanceId, State0, ChannelId, ChannelConfig
            ),
            Channel = Parameter1,
            Channels2 = Channels#{ChannelId => Channel},
            {ok, State#{channels => Channels2}}
    end.

on_remove_channel(InstanceId, #{channels := Channels} = OldState0, ChannelId) ->
    {ok, OldState} = emqx_bridge_http_connector:on_remove_channel(InstanceId, OldState0, ChannelId),
    Channels2 = maps:remove(ChannelId, Channels),
    {ok, OldState#{channels => Channels2}}.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_get_channel_status(_InstanceId, ChannelId, #{channels := Channels}) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            connected;
        _ ->
            {error, not_exists}
    end.

render_template([<<"update_without_doc_template">>], Msg) ->
    emqx_utils_json:encode(#{<<"doc">> => Msg});
render_template([<<"create_without_doc_template">>], Msg) ->
    emqx_utils_json:encode(#{<<"doc">> => Msg, <<"doc_as_upsert">> => true});
render_template(Template, Msg) ->
    % Ignoring errors here, undefined bindings will be replaced with empty string.
    Opts = #{var_trans => fun to_string/2},
    {String, _Errors} = emqx_template:render(Template, {emqx_jsonish, Msg}, Opts),
    String.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

to_string(Name, Value) ->
    emqx_template:to_string(render_var(Name, Value)).
render_var(_, undefined) ->
    % NOTE Any allowed but undefined binding will be replaced with empty string
    <<>>;
render_var(_Name, Value) ->
    Value.
%% delete DELETE /<index>/_doc/<_id>
path(#{action := delete, id := Id, index := Index} = Action) ->
    BasePath = ["/", Index, "/_doc/", Id],
    Qs = add_query_string([routing], Action),
    BasePath ++ Qs;
%% update POST /<index>/_update/<_id>
path(#{action := update, id := Id, index := Index} = Action) ->
    BasePath = ["/", Index, "/_update/", Id],
    Qs = add_query_string([routing, require_alias], Action),
    BasePath ++ Qs;
%% create with id  /<index>/_doc/_id
path(#{action := create, index := Index, id := Id} = Action) ->
    BasePath = ["/", Index, "/_doc/", Id],
    Qs =
        case maps:get(overwrite, Action, true) of
            true ->
                add_query_string([routing, require_alias], Action);
            false ->
                Action1 = Action#{op_type => "create"},
                add_query_string([routing, require_alias, op_type], Action1)
        end,
    BasePath ++ Qs;
%% create without id POST /<index>/_doc/
path(#{action := create, index := Index} = Action) ->
    BasePath = ["/", Index, "/_doc/"],
    Qs = add_query_string([routing, require_alias], Action),
    BasePath ++ Qs.

method(#{action := create}) -> <<"POST">>;
method(#{action := delete}) -> <<"DELETE">>;
method(#{action := update}) -> <<"POST">>.

add_query_string(Keys, Param0) ->
    Param1 = maps:with(Keys, Param0),
    FoldFun = fun(K, V, Acc) -> [[atom_to_list(K), "=", to_str(V)] | Acc] end,
    case maps:fold(FoldFun, [], Param1) of
        "" -> "";
        QString -> "?" ++ lists:join("&", QString)
    end.

to_str(List) when is_list(List) -> List;
to_str(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_str(false) -> "false";
to_str(true) -> "true";
to_str(Atom) when is_atom(Atom) -> atom_to_list(Atom).

handle_response({ok, Code, _Headers, _Body} = Resp) when Code =:= 200; Code =:= 201 ->
    Resp;
handle_response({ok, Code, _Body} = Resp) when Code =:= 200; Code =:= 201 ->
    Resp;
handle_response({ok, Code, _Headers, Body}) ->
    {error, #{code => Code, body => Body}};
handle_response({ok, Code, Body}) ->
    {error, #{code => Code, body => Body}};
handle_response({error, _} = Error) ->
    Error.

get_body_template(#{action := update, doc := Doc} = Template) ->
    case maps:get(doc_as_upsert, Template, false) of
        false -> <<"{\"doc\":", Doc/binary, "}">>;
        true -> <<"{\"doc\":", Doc/binary, ",\"doc_as_upsert\": true}">>
    end;
get_body_template(#{action := update} = Template) ->
    case maps:get(doc_as_upsert, Template, false) of
        false -> <<"update_without_doc_template">>;
        true -> <<"create_without_doc_template">>
    end;
get_body_template(#{doc := Doc}) ->
    Doc;
get_body_template(_) ->
    undefined.
