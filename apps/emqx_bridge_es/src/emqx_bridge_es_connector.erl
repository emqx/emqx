%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_es_connector).

-behaviour(emqx_resource).

-include("emqx_bridge_es.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% `emqx_resource' API
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_get_status/2,
    on_query/3,
    on_query_async/4,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    connector_examples/1,
    connector_example_values/0
]).

%% emqx_connector_resource behaviour callbacks
-export([connector_config/2]).

-type config() ::
    #{
        base_url := #{
            scheme := http | https,
            host := iolist(),
            port := inet:port_number(),
            path := _
        },
        connect_timeout := pos_integer(),
        pool_type := random | hash,
        pool_size := pos_integer(),
        request => undefined | map(),
        atom() => _
    }.

-type state() ::
    #{
        base_path := _,
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
        base_url => <<"http://127.0.0.1:9200/">>,
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
        {base_url,
            ?HOCON(
                emqx_schema:url(),
                #{
                    required => true,
                    desc => ?DESC(emqx_bridge_es, "config_base_url")
                }
            )},
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
desc(_) ->
    undefined.

connector_config(Conf, #{name := Name, parse_confs := ParseConfs}) ->
    #{
        base_url := BaseUrl,
        authentication :=
            #{
                username := Username,
                password := Password0
            }
    } = Conf,

    Password = emqx_secret:unwrap(Password0),
    Base64 = base64:encode(<<Username/binary, ":", Password/binary>>),
    BasicToken = <<"Basic ", Base64/binary>>,

    WebhookConfig =
        Conf#{
            method => <<"post">>,
            url => BaseUrl,
            headers => [
                {<<"Content-type">>, <<"application/json">>},
                {<<"Authorization">>, BasicToken}
            ]
        },
    ParseConfs(
        <<"http">>,
        Name,
        WebhookConfig
    ).

%%-------------------------------------------------------------------------------------
%% `emqx_resource' API
%%-------------------------------------------------------------------------------------
callback_mode() -> async_if_possible.

-spec on_start(manager_id(), config()) -> {ok, state()} | no_return().
on_start(InstanceId, Config) ->
    case emqx_bridge_http_connector:on_start(InstanceId, Config) of
        {ok, State} ->
            ?SLOG(info, #{
                msg => "elasticsearch_bridge_started",
                instance_id => InstanceId,
                request => maps:get(request, State, <<>>)
            }),
            ?tp(elasticsearch_bridge_started, #{instance_id => InstanceId}),
            {ok, State#{channels => #{}}};
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_start_elasticsearch_bridge",
                instance_id => InstanceId,
                request => maps:get(request, Config, <<>>),
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
    {connected, state()} | {disconnected, state(), term()}.
on_get_status(InstanceId, State) ->
    emqx_bridge_http_connector:on_get_status(InstanceId, State).

-spec on_query(manager_id(), tuple(), state()) ->
    {ok, pos_integer(), [term()], term()}
    | {ok, pos_integer(), [term()]}
    | {error, term()}.
on_query(InstanceId, {ChannelId, Msg} = Req, #{channels := Channels} = State) ->
    ?tp(elasticsearch_bridge_on_query, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "elasticsearch_bridge_on_query_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),
    case try_render_message(Req, Channels) of
        {ok, Body} ->
            handle_response(
                emqx_bridge_http_connector:on_query(
                    InstanceId, {ChannelId, {Msg, Body}}, State
                )
            );
        Error ->
            Error
    end.

-spec on_query_async(manager_id(), tuple(), {function(), [term()]}, state()) ->
    {ok, pid()} | {error, empty_request}.
on_query_async(
    InstanceId, {ChannelId, Msg} = Req, ReplyFunAndArgs0, #{channels := Channels} = State
) ->
    ?tp(elasticsearch_bridge_on_query_async, #{instance_id => InstanceId}),
    ?SLOG(debug, #{
        msg => "elasticsearch_bridge_on_query_async_called",
        instance_id => InstanceId,
        send_message => Req,
        state => emqx_utils:redact(State)
    }),
    case try_render_message(Req, Channels) of
        {ok, Payload} ->
            ReplyFunAndArgs =
                {
                    fun(Result) ->
                        Response = handle_response(Result),
                        emqx_resource:apply_reply_fun(ReplyFunAndArgs0, Response)
                    end,
                    []
                },
            emqx_bridge_http_connector:on_query_async(
                InstanceId, {ChannelId, {Msg, Payload}}, ReplyFunAndArgs, State
            );
        Error ->
            Error
    end.

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
            #{data := Data} = Parameter,
            Parameter1 = Parameter#{path => path(Parameter), method => <<"post">>},
            {ok, State} = emqx_bridge_http_connector:on_add_channel(
                InstanceId, State0, ChannelId, #{parameters => Parameter1}
            ),
            case preproc_data_template(Data) of
                [] ->
                    {error, invalid_data};
                DataTemplate ->
                    Channel = Parameter1#{data => DataTemplate},
                    Channels2 = Channels#{ChannelId => Channel},
                    {ok, State#{channels => Channels2}}
            end
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

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------
path(Param) ->
    Target = maps:get(target, Param, undefined),
    QString0 = maps:fold(
        fun(K, V, Acc) ->
            [[atom_to_list(K), "=", to_str(V)] | Acc]
        end,
        [["_source=false"], ["filter_path=items.*.error"]],
        maps:with([require_alias, routing, wait_for_active_shards], Param)
    ),
    QString = "?" ++ lists:join("&", QString0),
    target(Target) ++ QString.

target(undefined) -> "/_bulk";
target(Str) -> "/" ++ binary_to_list(Str) ++ "/_bulk".

to_str(List) when is_list(List) -> List;
to_str(false) -> "false";
to_str(true) -> "true";
to_str(Atom) when is_atom(Atom) -> atom_to_list(Atom).

proc_data(DataList, Msg) when is_list(DataList) ->
    [
        begin
            proc_data(Data, Msg)
        end
     || Data <- DataList
    ];
proc_data(
    #{
        action := Action,
        '_index' := IndexT,
        '_id' := IdT,
        require_alias := RequiredAliasT,
        fields := FieldsT
    },
    Msg
) when Action =:= create; Action =:= index ->
    [
        emqx_utils_json:encode(
            #{
                Action => filter([
                    {'_index', emqx_placeholder:proc_tmpl(IndexT, Msg)},
                    {'_id', emqx_placeholder:proc_tmpl(IdT, Msg)},
                    {required_alias, emqx_placeholder:proc_tmpl(RequiredAliasT, Msg)}
                ])
            }
        ),
        "\n",
        emqx_placeholder:proc_tmpl(FieldsT, Msg),
        "\n"
    ];
proc_data(
    #{
        action := delete,
        '_index' := IndexT,
        '_id' := IdT,
        require_alias := RequiredAliasT
    },
    Msg
) ->
    [
        emqx_utils_json:encode(
            #{
                delete => filter([
                    {'_index', emqx_placeholder:proc_tmpl(IndexT, Msg)},
                    {'_id', emqx_placeholder:proc_tmpl(IdT, Msg)},
                    {required_alias, emqx_placeholder:proc_tmpl(RequiredAliasT, Msg)}
                ])
            }
        ),
        "\n"
    ];
proc_data(
    #{
        action := update,
        '_index' := IndexT,
        '_id' := IdT,
        require_alias := RequiredAliasT,
        doc_as_upsert := DocAsUpsert,
        upsert := Upsert,
        fields := FieldsT
    },
    Msg
) ->
    [
        emqx_utils_json:encode(
            #{
                update => filter([
                    {'_index', emqx_placeholder:proc_tmpl(IndexT, Msg)},
                    {'_id', emqx_placeholder:proc_tmpl(IdT, Msg)},
                    {required_alias, emqx_placeholder:proc_tmpl(RequiredAliasT, Msg)},
                    {doc_as_upsert, emqx_placeholder:proc_tmpl(DocAsUpsert, Msg)},
                    {upsert, emqx_placeholder:proc_tmpl(Upsert, Msg)}
                ])
            }
        ),
        "\n{\"doc\":",
        emqx_placeholder:proc_tmpl(FieldsT, Msg),
        "}\n"
    ].

filter(List) ->
    Fun = fun
        ({_K, V}) when V =:= undefined; V =:= <<"undefined">>; V =:= "undefined" ->
            false;
        ({_K, V}) when V =:= ""; V =:= <<>> ->
            false;
        ({_K, V}) when V =:= "false" -> {true, false};
        ({_K, V}) when V =:= "true" -> {true, true};
        ({_K, _V}) ->
            true
    end,
    maps:from_list(lists:filtermap(Fun, List)).

handle_response({ok, 200, _Headers, Body} = Resp) ->
    eval_response_body(Body, Resp);
handle_response({ok, 200, Body} = Resp) ->
    eval_response_body(Body, Resp);
handle_response({ok, Code, _Headers, Body}) ->
    {error, #{code => Code, body => Body}};
handle_response({ok, Code, Body}) ->
    {error, #{code => Code, body => Body}};
handle_response({error, _} = Error) ->
    Error.

eval_response_body(<<"{}">>, Resp) -> Resp;
eval_response_body(Body, _Resp) -> {error, emqx_utils_json:decode(Body)}.

preproc_data_template(DataList) when is_list(DataList) ->
    [
        begin
            preproc_data_template(Data)
        end
     || Data <- DataList
    ];
preproc_data_template(#{action := create} = Data) ->
    Index = maps:get('_index', Data, ""),
    Id = maps:get('_id', Data, ""),
    RequiredAlias = maps:get(require_alias, Data, ""),
    Fields = maps:get(fields, Data, ""),
    #{
        action => create,
        '_index' => emqx_placeholder:preproc_tmpl(Index),
        '_id' => emqx_placeholder:preproc_tmpl(Id),
        require_alias => emqx_placeholder:preproc_tmpl(RequiredAlias),
        fields => emqx_placeholder:preproc_tmpl(Fields)
    };
preproc_data_template(#{action := index} = Data) ->
    Data1 = preproc_data_template(Data#{action => create}),
    Data1#{action => index};
preproc_data_template(#{action := delete} = Data) ->
    Data1 = preproc_data_template(Data#{action => create}),
    Data2 = Data1#{action => delete},
    maps:remove(fields, Data2);
preproc_data_template(#{action := update} = Data) ->
    Data1 = preproc_data_template(Data#{action => index}),
    DocAsUpsert = maps:get(doc_as_upsert, Data, ""),
    Upsert = maps:get(upsert, Data, ""),
    Data1#{
        action => update,
        doc_as_upsert => emqx_placeholder:preproc_tmpl(DocAsUpsert),
        upsert => emqx_placeholder:preproc_tmpl(Upsert)
    }.

try_render_message({ChannelId, Msg}, Channels) ->
    case maps:find(ChannelId, Channels) of
        {ok, #{data := Data}} ->
            {ok, proc_data(Data, Msg)};
        _ ->
            {error, {unrecoverable_error, {invalid_channel_id, ChannelId}}}
    end.
