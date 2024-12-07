%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_opents_connector).

-behaviour(emqx_connector_examples).

-behaviour(emqx_resource).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([namespace/0, roots/0, fields/1, desc/1]).

%% `emqx_resource' API
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_batch_query/3,
    on_get_status/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channels/1,
    on_get_channel_status/3,
    on_format_query_result/1
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

resource_type() -> opents.

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
    [{ChannelId, _} | _] = BatchReq,
    case try_render_messages(BatchReq, Channels) of
        {ok, Datas} ->
            do_query(InstanceId, ChannelId, Datas, State);
        Error ->
            Error
    end.

on_format_query_result({ok, StatusCode, BodyMap}) ->
    #{result => ok, status_code => StatusCode, body => BodyMap};
on_format_query_result(Result) ->
    Result.

on_get_status(_InstanceId, #{server := Server}) ->
    case opentsdb_connectivity(Server) of
        ok ->
            ?status_connected;
        {error, Reason} ->
            ?SLOG(error, #{msg => "opents_lost_connection", reason => Reason}),
            ?status_connecting
    end.

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

do_query(InstanceId, ChannelID, Query, #{pool_name := PoolName} = State) ->
    ?TRACE(
        "QUERY",
        "opents_connector_received",
        #{connector => InstanceId, query => Query, state => State}
    ),
    emqx_trace:rendered_action_template(ChannelID, #{query => Query}),

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
        fun(
            #{
                metric := MetricTk,
                tags := TagsProcer,
                value := ValueProcer,
                timestamp := TimeProcer
            },
            InAcc
        ) ->
            MetricVal = emqx_placeholder:proc_tmpl(MetricTk, Msg),
            TagsVal = TagsProcer(Msg, RawOpts),
            ValueVal = ValueProcer(Msg, RawOpts),
            Result = TimeProcer(Msg, #{metric => MetricVal, tags => TagsVal, value => ValueVal}),
            [Result | InAcc]
        end,
        Acc,
        DataList
    ).

preproc_data_template([]) ->
    preproc_data_template(emqx_bridge_opents:default_data_template());
preproc_data_template(DataList) ->
    lists:map(
        fun(#{metric := Metric, tags := Tags, value := Value} = Data) ->
            TagsProcer = mk_tags_procer(Tags),
            ValueProcer = mk_value_procer(Value),
            #{
                metric => emqx_placeholder:preproc_tmpl(Metric),
                tags => TagsProcer,
                value => ValueProcer,
                timestamp => mk_timestamp_procer(Data)
            }
        end,
        DataList
    ).

mk_tags_procer(Tmpl) when is_binary(Tmpl) ->
    TagsTks = emqx_placeholder:preproc_tmpl(Tmpl),
    fun(Msg, RawOpts) ->
        case emqx_placeholder:proc_tmpl(TagsTks, Msg, RawOpts) of
            [undefined] ->
                #{};
            [Any] ->
                Any
        end
    end;
mk_tags_procer(Map) when is_map(Map) ->
    TagTkList = [
        {
            emqx_placeholder:preproc_tmpl(emqx_utils_conv:bin(TagName)),
            emqx_placeholder:preproc_tmpl(TagValue)
        }
     || {TagName, TagValue} <- maps:to_list(Map)
    ],
    fun(Msg, _RawOpts) ->
        maps:from_list([
            {
                emqx_placeholder:proc_tmpl(TagName, Msg),
                emqx_placeholder:proc_tmpl(TagValue, Msg)
            }
         || {TagName, TagValue} <- TagTkList
        ])
    end.

mk_value_procer(Text) when is_binary(Text) ->
    ValueTk = emqx_placeholder:preproc_tmpl(Text),
    case ValueTk of
        [_] ->
            %% just one element, maybe is a variable or a plain text
            %% we should keep it as it is
            fun(Msg, RawOpts) ->
                erlang:hd(emqx_placeholder:proc_tmpl(ValueTk, Msg, RawOpts))
            end;
        Tks when is_list(Tks) ->
            fun(Msg, _RawOpts) ->
                emqx_placeholder:proc_tmpl(Tks, Msg)
            end
    end;
mk_value_procer(Raw) ->
    fun(_, _) ->
        Raw
    end.

mk_timestamp_procer(#{timestamp := Timestamp}) ->
    TimestampTk = emqx_placeholder:preproc_tmpl(Timestamp),
    fun(Msg, Base) ->
        Base#{timestamp => emqx_placeholder:proc_tmpl(TimestampTk, Msg)}
    end;
mk_timestamp_procer(_) ->
    fun(_Msg, Base) ->
        Base
    end.
