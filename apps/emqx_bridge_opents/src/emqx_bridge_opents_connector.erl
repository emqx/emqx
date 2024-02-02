%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_opents_connector).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, desc/1]).

%% `emqx_resource' API
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3
]).

-export([connector_examples/1]).

-export([connect/1]).

-import(hoconsc, [mk/2, enum/1, ref/2]).

-define(CONNECTOR_TYPE, opents).

namespace() -> "opents_connector".

%%=====================================================================
%% V1 Hocon schema
roots() ->
    [{config, #{type => hoconsc:ref(?MODULE, config)}}].

fields(config) ->
    [
        {server, mk(binary(), #{required => true, desc => ?DESC("server")})},
        {pool_size, fun emqx_connector_schema_lib:pool_size/1},
        {summary, mk(boolean(), #{default => true, desc => ?DESC("summary")})},
        {details, mk(boolean(), #{default => false, desc => ?DESC("details")})},
        {auto_reconnect, fun emqx_connector_schema_lib:auto_reconnect/1}
    ];
%%=====================================================================
%% V2 Hocon schema

fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        proplists_without([auto_reconnect], fields(config)) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("post") ->
    emqx_connector_schema:type_and_name_fields(enum([opents])) ++ fields("config_connector");
fields("put") ->
    fields("config_connector");
fields("get") ->
    emqx_bridge_schema:status_fields() ++ fields("post").

desc(config) ->
    ?DESC("desc_config");
desc(connector_resource_opts) ->
    ?DESC(emqx_resource_schema, "resource_opts");
desc("config_connector") ->
    ?DESC("desc_config");
desc(Method) when Method =:= "get"; Method =:= "put"; Method =:= "post" ->
    ["Configuration for OpenTSDB using `", string:to_upper(Method), "` method."];
desc(_) ->
    undefined.

proplists_without(Keys, List) ->
    [El || El = {K, _} <- List, not lists:member(K, Keys)].

%%=====================================================================
%% V2 examples
connector_examples(Method) ->
    [
        #{
            <<"opents">> =>
                #{
                    summary => <<"OpenTSDB Connector">>,
                    value => emqx_connector_schema:connector_values(
                        Method, ?CONNECTOR_TYPE, connector_example_values()
                    )
                }
        }
    ].

connector_example_values() ->
    #{
        name => <<"opents_connector">>,
        type => opents,
        enable => true,
        server => <<"http://localhost:4242/">>,
        pool_size => 8
    }.

%%========================================================================================
%% `emqx_resource' API
%%========================================================================================

-define(HTTP_CONNECT_TIMEOUT, 1000).

callback_mode() -> always_sync.

on_start(
    InstanceId,
    #{
        server := Server,
        pool_size := PoolSize,
        summary := Summary,
        details := Details
    } = Config
) ->
    ?SLOG(info, #{
        msg => "starting_opents_connector",
        connector => InstanceId,
        config => emqx_utils:redact(Config)
    }),

    Options = [
        {server, to_str(Server)},
        {summary, Summary},
        {details, Details},
        {pool_size, PoolSize}
    ],

    State = #{pool_name => InstanceId, server => Server, channels => #{}},
    case opentsdb_connectivity(Server) of
        ok ->
            case emqx_resource_pool:start(InstanceId, ?MODULE, Options) of
                ok ->
                    {ok, State};
                Error ->
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(error, #{msg => "Initiate resource failed", reason => Reason}),
            Error
    end.

on_stop(InstanceId, _State) ->
    ?SLOG(info, #{
        msg => "stopping_opents_connector",
        connector => InstanceId
    }),
    ?tp(opents_bridge_stopped, #{instance_id => InstanceId}),
    emqx_resource_pool:stop(InstanceId).

on_query(InstanceId, Request, State) ->
    on_batch_query(InstanceId, [Request], State).

on_batch_query(
    InstanceId,
    BatchReq,
    #{channels := Channels} = State
) ->
    case try_render_messages(BatchReq, Channels) of
        {ok, Datas} ->
            do_query(InstanceId, Datas, State);
        Error ->
            Error
    end.

on_get_status(_InstanceId, #{server := Server}) ->
    Result =
        case opentsdb_connectivity(Server) of
            ok ->
                connected;
            {error, Reason} ->
                ?SLOG(error, #{msg => "opents_lost_connection", reason => Reason}),
                connecting
        end,
    Result.

on_add_channel(
    _InstanceId,
    #{channels := Channels} = OldState,
    ChannelId,
    #{
        parameters := #{data := Data} = Parameter
    }
) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            {error, already_exists};
        _ ->
            Channel = Parameter#{
                data := preproc_data_template(Data)
            },
            Channels2 = Channels#{ChannelId => Channel},
            {ok, OldState#{channels := Channels2}}
    end.

on_remove_channel(_InstanceId, #{channels := Channels} = OldState, ChannelId) ->
    {ok, OldState#{channels => maps:remove(ChannelId, Channels)}}.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_get_channel_status(InstanceId, ChannelId, #{channels := Channels} = State) ->
    case maps:is_key(ChannelId, Channels) of
        true ->
            on_get_status(InstanceId, State);
        _ ->
            {error, not_exists}
    end.

%%========================================================================================
%% Helper fns
%%========================================================================================

do_query(InstanceId, Query, #{pool_name := PoolName} = State) ->
    ?TRACE(
        "QUERY",
        "opents_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),

    ?tp(opents_bridge_on_query, #{instance_id => InstanceId}),

    Result = ecpool:pick_and_do(PoolName, {opentsdb, put, [Query]}, no_handover),

    case Result of
        {error, Reason} ->
            ?tp(
                opents_connector_query_return,
                #{error => Reason}
            ),
            ?SLOG(error, #{
                msg => "opents_connector_do_query_failed",
                connector => InstanceId,
                query => Query,
                reason => Reason
            }),
            case Reason of
                ecpool_empty ->
                    {error, {recoverable_error, Reason}};
                _ ->
                    Result
            end;
        _ ->
            ?tp(
                opents_connector_query_return,
                #{result => Result}
            ),
            Result
    end.

connect(Opts) ->
    opentsdb:start_link(Opts).

to_str(List) when is_list(List) ->
    List;
to_str(Bin) when is_binary(Bin) ->
    erlang:binary_to_list(Bin).

opentsdb_connectivity(Server) ->
    SvrUrl =
        case Server of
            <<"http://", _/binary>> -> Server;
            <<"https://", _/binary>> -> Server;
            _ -> "http://" ++ Server
        end,
    emqx_connector_lib:http_connectivity(SvrUrl, ?HTTP_CONNECT_TIMEOUT).

try_render_messages([{ChannelId, _} | _] = BatchReq, Channels) ->
    case maps:find(ChannelId, Channels) of
        {ok, Channel} ->
            {ok,
                lists:foldl(
                    fun({_, Message}, Acc) ->
                        render_channel_message(Message, Channel, Acc)
                    end,
                    [],
                    BatchReq
                )};
        _ ->
            {error, {unrecoverable_error, {invalid_channel_id, ChannelId}}}
    end.

render_channel_message(Msg, #{data := DataList}, Acc) ->
    RawOpts = #{return => rawlist, var_trans => fun(X) -> X end},
    lists:foldl(
        fun(#{metric := MetricTk, tags := TagsTk, value := ValueTk} = Data, InAcc) ->
            MetricVal = emqx_placeholder:proc_tmpl(MetricTk, Msg),

            TagsVal =
                case TagsTk of
                    [tags | TagTkList] ->
                        maps:from_list([
                            {
                                emqx_placeholder:proc_tmpl(TagName, Msg),
                                emqx_placeholder:proc_tmpl(TagValue, Msg)
                            }
                         || {TagName, TagValue} <- TagTkList
                        ]);
                    TagsTks ->
                        case emqx_placeholder:proc_tmpl(TagsTks, Msg, RawOpts) of
                            [undefined] ->
                                #{};
                            [Any] ->
                                Any
                        end
                end,

            ValueVal =
                case ValueTk of
                    [_] ->
                        %% just one element, maybe is a variable or a plain text
                        %% we should keep it as it is
                        erlang:hd(emqx_placeholder:proc_tmpl(ValueTk, Msg, RawOpts));
                    Tks when is_list(Tks) ->
                        emqx_placeholder:proc_tmpl(Tks, Msg);
                    Raw ->
                        %% not a token list, just a raw value
                        Raw
                end,
            Base = #{metric => MetricVal, tags => TagsVal, value => ValueVal},
            [
                case maps:get(timestamp, Data, undefined) of
                    undefined ->
                        Base;
                    TimestampTk ->
                        Base#{timestamp => emqx_placeholder:proc_tmpl(TimestampTk, Msg)}
                end
                | InAcc
            ]
        end,
        Acc,
        DataList
    ).

preproc_data_template([]) ->
    preproc_data_template(emqx_bridge_opents:default_data_template());
preproc_data_template(DataList) ->
    lists:map(
        fun(#{tags := Tags, value := Value} = Data) ->
            Data2 = maps:without([tags, value], Data),
            Template = maps:map(
                fun(_Key, Val) ->
                    emqx_placeholder:preproc_tmpl(Val)
                end,
                Data2
            ),

            TagsTk =
                case Tags of
                    Tmpl when is_binary(Tmpl) ->
                        emqx_placeholder:preproc_tmpl(Tmpl);
                    Map when is_map(Map) ->
                        [
                            tags
                            | [
                                {
                                    emqx_placeholder:preproc_tmpl(emqx_utils_conv:bin(TagName)),
                                    emqx_placeholder:preproc_tmpl(TagValue)
                                }
                             || {TagName, TagValue} <- maps:to_list(Map)
                            ]
                        ]
                end,

            ValueTk =
                case Value of
                    Text when is_binary(Text) ->
                        emqx_placeholder:preproc_tmpl(Text);
                    Raw ->
                        Raw
                end,

            Template#{tags => TagsTk, value => ValueTk}
        end,
        DataList
    ).
