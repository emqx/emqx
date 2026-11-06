%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_influxdb_connector).

-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(hoconsc, [mk/2, enum/1, ref/2]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    resource_type/0,
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_add_channel/4,
    on_remove_channel/3,
    on_get_channel_status/3,
    on_get_channels/1,
    on_query/3,
    on_batch_query/3,
    on_query_async/4,
    on_batch_query_async/4,
    on_get_status/2,
    on_format_query_result/1
]).
-export([reply_callback/2]).
-export([coordinated_reply_callback/3]).

-export([
    roots/0,
    namespace/0,
    fields/1,
    desc/1
]).

-export([precision_field/0]).

%% only for test
-export([client_config/2, is_unrecoverable_error/1, data_to_points/2, render_bucket/2]).

-type ts_precision() :: ns | us | ms | s.

%% Allocatable resources
-define(influx_client, influx_client).

-define(INFLUXDB_DEFAULT_PORT, 8086).

%% influxdb servers don't need parse
-define(INFLUXDB_HOST_OPTIONS, #{
    default_port => ?INFLUXDB_DEFAULT_PORT
}).

-define(DEFAULT_TIMESTAMP_TMPL, "${timestamp}").

-define(set_tag, set_tag).
-define(set_field, set_field).

-define(IS_HTTP_ERROR(STATUS_CODE),
    (is_integer(STATUS_CODE) andalso
        (STATUS_CODE < 200 orelse STATUS_CODE >= 300))
).

-define(DEFAULT_POOL_SIZE, 8).

-define(MIN_INT64, -16#8000000000000000).
-define(MAX_INT64, 16#7FFFFFFFFFFFFFFF).
-define(MAX_UINT64, 16#FFFFFFFFFFFFFFFF).

%% -------------------------------------------------------------------------------------------------
%% resource callback
resource_type() -> influxdb.

callback_mode() -> async_if_possible.

on_add_channel(
    _InstanceId,
    #{channels := Channels, client := Client} = OldState,
    ChannelId,
    #{parameters := Parameters} = ChannelConfig0
) ->
    Precision = maps:get(precision, Parameters, ms),
    ChannelConfig = maps:merge(
        Parameters,
        ChannelConfig0#{
            channel_client => influxdb:update_precision(Client, Precision),
            write_syntax => to_config(write_config(Parameters), Precision),
            bucket_tmpl => preproc_bucket_tmpl(Parameters)
        }
    ),
    {ok, OldState#{
        channels => maps:put(ChannelId, ChannelConfig, Channels)
    }}.

on_remove_channel(_InstanceId, #{channels := Channels} = State, ChannelId) ->
    NewState = State#{channels => maps:remove(ChannelId, Channels)},
    {ok, NewState}.

on_get_channel_status(_InstanceId, _ChannelId, _State) ->
    ?status_connected.

on_get_channels(InstanceId) ->
    emqx_bridge_v2:get_channels_for_connector(InstanceId).

on_start(InstId, Config) ->
    %% InstID as pool would be handled by influxdb client
    %% so there is no need to allocate pool_name here
    %% ehttpc for influxdb-v1/v2,
    %% ecpool for influxdb-udp
    %% See: influxdb:start_client/1
    start_client(InstId, Config).

on_stop(InstId, _State) ->
    case emqx_resource:get_allocated_resources(InstId) of
        #{?influx_client := Client} ->
            Res = influxdb:stop_client(Client),
            ?tp(influxdb_client_stopped, #{instance_id => InstId}),
            Res;
        _ ->
            ok
    end.

on_query(InstId, {Channel, Message}, #{channels := ChannelConf}) ->
    ChannelConfig = maps:get(Channel, ChannelConf),
    #{write_syntax := SyntaxLines, channel_client := Client} = ChannelConfig,
    case data_to_points(Message, SyntaxLines) of
        {ok, Points} ->
            Client1 = render_bucket_client(Message, Client, ChannelConfig),
            ?tp(
                influxdb_connector_send_query,
                #{
                    points => Points,
                    batch => false,
                    mode => sync,
                    path => trace_path(Client1)
                }
            ),
            do_query(InstId, Channel, Client1, Points);
        {error, ErrorPoints} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => false, mode => sync, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            {error, {unrecoverable_error, ErrorPoints}}
    end.

%% Once a Batched Data trans to points failed.
%% This batch query failed
on_batch_query(InstId, BatchData, #{channels := ChannelConf}) ->
    [{Channel, _} | _] = BatchData,
    ChannelConfig = maps:get(Channel, ChannelConf),
    #{write_syntax := SyntaxLines} = ChannelConfig,
    case group_batch_by_bucket(BatchData, ChannelConfig) of
        [{Client, Data}] ->
            case parse_batch_data(InstId, Data, SyntaxLines) of
                {ok, Points} ->
                    ?tp(
                        influxdb_connector_send_query,
                        #{
                            points => Points,
                            batch => true,
                            mode => sync,
                            path => trace_path(Client)
                        }
                    ),
                    do_query(InstId, Channel, Client, Points);
                {error, Reason} ->
                    ?tp(
                        influxdb_connector_send_query_error,
                        #{batch => true, mode => sync, error => Reason}
                    ),
                    {error, {unrecoverable_error, Reason}}
            end;
        Groups ->
            do_grouped_batch_query(InstId, Channel, Groups, SyntaxLines)
    end.

on_query_async(
    InstId,
    {Channel, Message},
    {ReplyFun, Args},
    #{channels := ChannelConf}
) ->
    ChannelConfig = maps:get(Channel, ChannelConf),
    #{write_syntax := SyntaxLines, channel_client := Client} = ChannelConfig,
    case data_to_points(Message, SyntaxLines) of
        {ok, Points} ->
            Client1 = render_bucket_client(Message, Client, ChannelConfig),
            ?tp(
                influxdb_connector_send_query,
                #{
                    points => Points,
                    batch => false,
                    mode => async,
                    path => trace_path(Client1)
                }
            ),
            do_async_query(InstId, Channel, Client1, Points, {ReplyFun, Args});
        {error, ErrorPoints} = Err ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => false, mode => async, error => ErrorPoints}
            ),
            log_error_points(InstId, ErrorPoints),
            Err
    end.

on_batch_query_async(
    InstId,
    BatchData,
    ReplyFunAndArgs,
    #{channels := ChannelConf}
) ->
    [{Channel, _} | _] = BatchData,
    ChannelConfig = maps:get(Channel, ChannelConf),
    #{write_syntax := SyntaxLines} = ChannelConfig,
    case group_batch_by_bucket(BatchData, ChannelConfig) of
        [{Client, Data}] ->
            case parse_batch_data(InstId, Data, SyntaxLines) of
                {ok, Points} ->
                    ?tp(
                        influxdb_connector_send_query,
                        #{
                            points => Points,
                            batch => true,
                            mode => async,
                            path => trace_path(Client)
                        }
                    ),
                    do_async_query(InstId, Channel, Client, Points, ReplyFunAndArgs);
                {error, Reason} ->
                    ?tp(
                        influxdb_connector_send_query_error,
                        #{batch => true, mode => async, error => Reason}
                    ),
                    {error, {unrecoverable_error, Reason}}
            end;
        Groups ->
            do_grouped_async_batch_query(
                InstId, Channel, Groups, SyntaxLines, ReplyFunAndArgs
            )
    end.

on_format_query_result(Result) ->
    emqx_bridge_http_connector:on_format_query_result(Result).

on_get_status(_InstId, #{client := Client}) ->
    ReturnReason = true,
    case influxdb:is_alive(Client, ReturnReason) of
        true ->
            case influxdb:check_auth(Client) of
                ok ->
                    ?status_connected;
                {error, Reason} ->
                    {?status_disconnected, Reason}
            end;
        false ->
            {?status_disconnected, ~"health check call failed"};
        {false, Reason} ->
            {?status_disconnected, Reason}
    end.

%% -------------------------------------------------------------------------------------------------
%% Dynamic bucket

preproc_bucket_tmpl(#{bucket := Bucket}) when is_binary(Bucket) ->
    emqx_placeholder:preproc_tmpl(Bucket);
preproc_bucket_tmpl(_) ->
    undefined.

render_bucket_client(Data, Client, ChannelConfig) ->
    case render_bucket(Data, maps:get(bucket_tmpl, ChannelConfig, undefined)) of
        {ok, Bucket} ->
            influxdb:update_bucket(Client, Bucket);
        undefined ->
            Client
    end.

render_bucket(_Data, undefined) ->
    undefined;
render_bucket(Data, Tmpl) ->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    case emqx_placeholder:proc_tmpl(Tmpl, Data, TransOptions) of
        Parts when is_list(Parts) ->
            %% a template may render to several parts (static text mixed with
            %% placeholders, e.g. `tenant-${payload.tenant}`); concatenate them
            %% into the bucket name. A missing part falls back to the static
            %% bucket from the connector config.
            case lists:member(undefined, Parts) of
                true ->
                    undefined;
                false ->
                    case iolist_to_binary([bin(Part) || Part <- Parts]) of
                        <<>> ->
                            undefined;
                        Bucket ->
                            {ok, Bucket}
                    end
            end;
        _ ->
            undefined
    end.

%% Group batch messages by their rendered bucket so that each group can be
%% written to its own bucket with a single write request.
%% Batch items are kept as {Channel, Message} pairs, as expected by parse_batch_data/3.
group_batch_by_bucket(BatchData, ChannelConfig) ->
    #{channel_client := Client} = ChannelConfig,
    BucketTmpl = maps:get(bucket_tmpl, ChannelConfig, undefined),
    Groups = maps:groups_from_list(
        fun({_Channel, Message}) ->
            bucket_group_key(Message, BucketTmpl)
        end,
        fun(Item) -> Item end,
        BatchData
    ),
    [
        {bucket_group_client(Key, Client), lists:reverse(Items)}
     || {Key, Items} <- maps:to_list(Groups)
    ].

bucket_group_key(Message, BucketTmpl) ->
    case render_bucket(Message, BucketTmpl) of
        {ok, Bucket} -> {ok, Bucket};
        undefined -> static
    end.

bucket_group_client(static, Client) ->
    Client;
bucket_group_client({ok, Bucket}, Client) ->
    influxdb:update_bucket(Client, Bucket).

do_grouped_batch_query(InstId, Channel, Groups, SyntaxLines) ->
    case parse_groups(InstId, Groups, SyntaxLines) of
        {ok, ParsedGroups} ->
            write_groups_sync(InstId, Channel, without_empty_groups(ParsedGroups));
        {error, Reason} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => true, mode => sync, error => Reason}
            ),
            {error, {unrecoverable_error, Reason}}
    end.

%% Every group is written; all results are aggregated after every group has been
%% attempted, so one failing bucket does not drop the others.  A recoverable
%% error takes precedence over an unrecoverable one: dropping the batch because
%% one bucket permanently failed would silently lose data for another bucket
%% that could have succeeded on retry.
write_groups_sync(_InstId, _Channel, []) ->
    ok;
write_groups_sync(InstId, Channel, ParsedGroups) ->
    GroupResults = lists:foldl(
        fun({Client, Points}, Acc) ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => true, mode => sync, path => trace_path(Client)}
            ),
            case do_query(InstId, Channel, Client, Points) of
                ok -> Acc;
                Result -> record_group_result(Result, Acc)
            end
        end,
        group_results_acc(),
        ParsedGroups
    ),
    grouped_result(GroupResults).

%% When a batch is split into multiple bucket groups, each group write replies
%% asynchronously. A coordinator process owns the group state, monitors every
%% ehttpc worker used by the groups and calls the batch reply fun exactly once.
%% Returning the coordinator PID lets the resource buffer monitor it as well.
%%
%% All groups are parsed before any write is issued, so a conversion failure in
%% one group cannot leave the other groups partially written. Groups that render
%% no points are not written at all.
%%
%% NOTE: if any group reports a recoverable write error the resource retries the
%% whole batch, so groups that already succeeded may be written again. Structured
%% points carry the message timestamp (the default `${timestamp}`) and are
%% idempotent; raw lines without an explicit timestamp are not. Per-group retry
%% would need resource-level sub-request tracking.
do_grouped_async_batch_query(InstId, Channel, Groups, SyntaxLines, ReplyFunAndArgs) ->
    case parse_groups(InstId, Groups, SyntaxLines) of
        {ok, ParsedGroups} ->
            write_groups_async(
                InstId, Channel, without_empty_groups(ParsedGroups), ReplyFunAndArgs
            );
        {error, Reason} ->
            ?tp(
                influxdb_connector_send_query_error,
                #{batch => true, mode => async, error => Reason}
            ),
            emqx_resource:apply_reply_fun(
                ReplyFunAndArgs, {error, {unrecoverable_error, Reason}}
            ),
            ok
    end.

write_groups_async(_InstId, _Channel, [], ReplyFunAndArgs) ->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, ok),
    ok;
write_groups_async(InstId, Channel, ParsedGroups, ReplyFunAndArgs) ->
    trace_rendered_groups(Channel, ParsedGroups, true),
    Owner = self(),
    Coordinator = spawn(fun() ->
        grouped_async_coordinator(Owner, InstId, ParsedGroups, ReplyFunAndArgs)
    end),
    {ok, Coordinator}.

grouped_async_coordinator(Owner, InstId, ParsedGroups, ReplyFunAndArgs) ->
    Grouped = lists:zip(lists:seq(1, length(ParsedGroups)), ParsedGroups),
    State0 = (group_results_acc())#{
        owner => Owner,
        owner_ref => monitor(process, Owner),
        pending => maps:from_keys([GroupRef || {GroupRef, _} <- Grouped], true),
        group_workers => #{},
        worker_groups => #{},
        workers => #{}
    },
    State = start_grouped_writes(InstId, Grouped, State0),
    wait_grouped_writes(ReplyFunAndArgs, State).

start_grouped_writes(InstId, Grouped, State0) ->
    lists:foldl(
        fun({GroupRef, {Client, Points}}, State) ->
            ?tp(
                influxdb_connector_send_query,
                #{points => Points, batch => true, mode => async, path => trace_path(Client)}
            ),
            WrappedReplyFunAndArgs = {
                fun ?MODULE:coordinated_reply_callback/3,
                [self(), GroupRef]
            },
            try influxdb:write_async(Client, Points, WrappedReplyFunAndArgs) of
                {ok, WorkerPid} when is_pid(WorkerPid) ->
                    associate_group_worker(GroupRef, WorkerPid, State);
                {error, Reason} ->
                    complete_group(GroupRef, {error, Reason}, State)
            catch
                Class:Reason:Stacktrace ->
                    ?SLOG(error, #{
                        msg => "influxdb_grouped_async_write_start_failed",
                        connector => InstId,
                        class => Class,
                        reason => Reason,
                        stacktrace => Stacktrace
                    }),
                    complete_group(
                        GroupRef, {error, {async_write_start_failed, Reason}}, State
                    )
            end
        end,
        State0,
        Grouped
    ).

trace_rendered_groups(Channel, ParsedGroups, IsAsync) ->
    lists:foreach(
        fun({_Client, Points}) ->
            emqx_trace:rendered_action_template(
                Channel, #{points => Points, is_async => IsAsync}
            )
        end,
        ParsedGroups
    ).

%% Convert every group before any write is issued. If any group fails to
%% convert, the whole batch fails and nothing has been written yet.
parse_groups(InstId, Groups, SyntaxLines) ->
    parse_groups(InstId, Groups, SyntaxLines, []).

parse_groups(_InstId, [], _SyntaxLines, Acc) ->
    {ok, lists:reverse(Acc)};
parse_groups(InstId, [{Client, Data} | Rest], SyntaxLines, Acc) ->
    case parse_batch_data(InstId, Data, SyntaxLines) of
        {ok, Points} ->
            parse_groups(InstId, Rest, SyntaxLines, [{Client, Points} | Acc]);
        {error, Reason} ->
            {error, Reason}
    end.

without_empty_groups(ParsedGroups) ->
    [{Client, Points} || {Client, Points} <- ParsedGroups, Points =/= []].

coordinated_reply_callback(Coordinator, GroupRef, Result) ->
    Coordinator ! {grouped_write_reply, GroupRef, Result},
    ok.

associate_group_worker(GroupRef, WorkerPid, State0) ->
    Workers0 = maps:get(workers, State0),
    WorkerGroups0 = maps:get(worker_groups, State0),
    {Workers, WorkerGroups1} =
        case maps:find(WorkerPid, Workers0) of
            {ok, _MRef} ->
                {Workers0, WorkerGroups0};
            error ->
                {
                    maps:put(WorkerPid, monitor(process, WorkerPid), Workers0),
                    maps:put(WorkerPid, [], WorkerGroups0)
                }
        end,
    WorkerGroups = maps:update_with(
        WorkerPid,
        fun(GroupRefs) -> [GroupRef | GroupRefs] end,
        [GroupRef],
        WorkerGroups1
    ),
    State0#{
        group_workers := maps:put(GroupRef, WorkerPid, maps:get(group_workers, State0)),
        worker_groups := WorkerGroups,
        workers := Workers
    }.

complete_group(GroupRef, Result, State0) ->
    Pending0 = maps:get(pending, State0),
    case maps:is_key(GroupRef, Pending0) of
        false ->
            State0;
        true ->
            State1 = remove_group_association(GroupRef, State0#{
                pending := maps:remove(GroupRef, Pending0)
            }),
            record_group_result(Result, State1)
    end.

remove_group_association(GroupRef, State0) ->
    GroupWorkers0 = maps:get(group_workers, State0),
    case maps:take(GroupRef, GroupWorkers0) of
        error ->
            State0;
        {WorkerPid, GroupWorkers} ->
            WorkerGroups0 = maps:get(worker_groups, State0),
            WorkerGroups = maps:update_with(
                WorkerPid,
                fun(GroupRefs) -> lists:delete(GroupRef, GroupRefs) end,
                [],
                WorkerGroups0
            ),
            State1 = State0#{
                group_workers := GroupWorkers,
                worker_groups := WorkerGroups
            },
            case maps:get(WorkerPid, WorkerGroups) of
                [] -> remove_worker_monitor(WorkerPid, State1);
                _ -> State1
            end
    end.

remove_worker_monitor(WorkerPid, State0) ->
    State1 = State0#{
        worker_groups := maps:remove(WorkerPid, maps:get(worker_groups, State0))
    },
    Workers0 = maps:get(workers, State1),
    case maps:take(WorkerPid, Workers0) of
        error ->
            State1;
        {MRef, Workers} ->
            _ = demonitor(MRef, [flush]),
            State1#{workers := Workers}
    end.

handle_worker_down(WorkerPid, Reason, State0) ->
    Workers0 = maps:get(workers, State0),
    case maps:take(WorkerPid, Workers0) of
        error ->
            State0;
        {_MRef, Workers} ->
            GroupRefs = maps:get(WorkerPid, maps:get(worker_groups, State0), []),
            ?tp(influxdb_connector_group_worker_down, #{
                worker => WorkerPid,
                reason => Reason,
                pending_groups => GroupRefs
            }),
            State1 = State0#{workers := Workers},
            lists:foldl(
                fun(GroupRef, State) ->
                    complete_group(
                        GroupRef, {error, {async_worker_down, Reason}}, State
                    )
                end,
                State1,
                GroupRefs
            )
    end.

wait_grouped_writes(
    ReplyFunAndArgs,
    State = #{owner := Owner, owner_ref := OwnerRef}
) ->
    case maps:size(maps:get(pending, State)) of
        0 ->
            emqx_resource:apply_reply_fun(ReplyFunAndArgs, grouped_result(State));
        _ ->
            receive
                {'DOWN', OwnerRef, process, Owner, _Reason} ->
                    %% The resource buffer stopped or restarted. No caller remains to reply.
                    ok;
                {grouped_write_reply, GroupRef, Result} ->
                    wait_grouped_writes(
                        ReplyFunAndArgs, complete_group(GroupRef, Result, State)
                    );
                {'DOWN', _MRef, process, WorkerPid, Reason} ->
                    case maps:is_key(WorkerPid, maps:get(workers, State)) of
                        true ->
                            wait_grouped_writes(
                                ReplyFunAndArgs,
                                handle_worker_down(WorkerPid, Reason, State)
                            );
                        false ->
                            wait_grouped_writes(ReplyFunAndArgs, State)
                    end;
                _Other ->
                    wait_grouped_writes(ReplyFunAndArgs, State)
            after infinity ->
                %% Elvis requires an after clause. Infinity keeps the wait event-driven.
                wait_grouped_writes(ReplyFunAndArgs, State)
            end
    end.

group_results_acc() ->
    #{}.

record_group_result(Result, Acc0) ->
    case classify_result(Result) of
        {error, {recoverable_error, _}} = Error ->
            put_first_group_error(recoverable_error, Error, Acc0);
        {error, {unrecoverable_error, _}} = Error ->
            put_first_group_error(unrecoverable_error, Error, Acc0);
        _ ->
            Acc0
    end.

put_first_group_error(Key, Error, Acc) ->
    emqx_utils_maps:put_new(Key, Error, Acc).

grouped_result(#{recoverable_error := Error}) ->
    Error;
grouped_result(#{unrecoverable_error := Error}) ->
    Error;
grouped_result(_) ->
    ok.

path(Client) ->
    maps:get(path, Client, undefined).

%% Keep credentials out of trace metadata. v1 query-string authentication
%% stores the username and password in the path query string.
trace_path(Client) ->
    case path(Client) of
        undefined ->
            undefined;
        Path when is_binary(Path) ->
            hd(binary:split(Path, <<"?">>));
        Path ->
            hd(string:split(Path, "?", leading))
    end.

%% -------------------------------------------------------------------------------------------------
%% schema
namespace() -> connector_influxdb.

roots() ->
    [
        {config, #{
            type => hoconsc:ref(?MODULE, "connector")
        }}
    ].

fields("connector") ->
    [
        server_field(),
        pool_size_field(),
        emqx_connector_schema:ehttpc_max_inactive_sc(),
        parameter_field()
    ] ++ emqx_connector_schema_lib:ssl_fields();
fields("connector_influxdb_api_v1") ->
    [influxdb_type_field(influxdb_api_v1) | influxdb_api_v1_fields()];
fields("connector_influxdb_api_v2") ->
    [influxdb_type_field(influxdb_api_v2) | influxdb_api_v2_fields()];
fields("connector_influxdb_api_v3") ->
    [influxdb_type_field(influxdb_api_v3) | influxdb_api_v3_fields()];
%% ============ begin: schema for old bridge configs ============
fields(influxdb_api_v1) ->
    fields(common) ++ influxdb_api_v1_fields();
fields(influxdb_api_v2) ->
    fields(common) ++ influxdb_api_v2_fields();
fields(common) ->
    [
        server_field(),
        precision_field(),
        pool_size_field()
    ] ++ emqx_connector_schema_lib:ssl_fields().
%% ============ end: schema for old bridge configs ============

influxdb_type_field(Type) ->
    {influxdb_type, #{
        required => true,
        type => Type,
        default => Type,
        desc => ?DESC(atom_to_list(Type))
    }}.

server_field() ->
    {server, server()}.

precision_field() ->
    {precision,
        %% The influxdb only supports these 4 precision:
        %% See "https://github.com/influxdata/influxdb/blob/
        %% 6b607288439a991261307518913eb6d4e280e0a7/models/points.go#L487" for
        %% more information.
        mk(enum([ns, us, ms, s]), #{
            required => false, default => ms, desc => ?DESC("precision")
        })}.

pool_size_field() ->
    {pool_size,
        mk(
            integer(),
            #{
                required => false,
                default => ?DEFAULT_POOL_SIZE,
                desc => ?DESC("pool_size")
            }
        )}.

parameter_field() ->
    {parameters,
        mk(
            hoconsc:union([
                ref(?MODULE, "connector_influxdb_api_v1"),
                ref(?MODULE, "connector_influxdb_api_v2"),
                ref(?MODULE, "connector_influxdb_api_v3")
            ]),
            #{required => true, desc => ?DESC("influxdb_parameters")}
        )}.

influxdb_api_v1_fields() ->
    [
        {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
        {username, mk(binary(), #{desc => ?DESC("username")})},
        {password, emqx_schema_secret:mk(#{desc => ?DESC("password")})},
        {ping_with_auth,
            mk(boolean(), #{required => false, default => false, desc => ?DESC("ping_with_auth")})}
    ].

influxdb_api_v2_fields() ->
    [
        {bucket, mk(binary(), #{required => true, desc => ?DESC("bucket")})},
        {org, mk(binary(), #{required => true, desc => ?DESC("org")})},
        {token, emqx_schema_secret:mk(#{required => true, desc => ?DESC("token")})},
        {ping_with_auth,
            mk(boolean(), #{required => false, default => false, desc => ?DESC("ping_with_auth")})}
    ].

influxdb_api_v3_fields() ->
    [
        {database, mk(binary(), #{required => true, desc => ?DESC("database")})},
        {token, emqx_schema_secret:mk(#{required => true, desc => ?DESC("token")})},
        {ping_with_auth,
            mk(boolean(), #{required => false, default => true, desc => ?DESC("ping_with_auth")})}
    ].

server() ->
    Meta = #{
        required => false,
        default => <<"127.0.0.1:8086">>,
        desc => ?DESC("server"),
        converter => fun convert_server/2
    },
    emqx_schema:servers_sc(Meta, ?INFLUXDB_HOST_OPTIONS).

desc(common) ->
    ?DESC("common");
desc(parameters) ->
    ?DESC("influxdb_parameters");
desc("influxdb_parameters") ->
    ?DESC("influxdb_parameters");
desc(influxdb_api_v1) ->
    ?DESC("influxdb_api_v1");
desc(influxdb_api_v2) ->
    ?DESC("influxdb_api_v2");
desc(influxdb_api_v3) ->
    ?DESC("influxdb_api_v3");
desc("connector") ->
    ?DESC("connector");
desc("connector_influxdb_api_v1") ->
    ?DESC("influxdb_api_v1");
desc("connector_influxdb_api_v2") ->
    ?DESC("influxdb_api_v2");
desc("connector_influxdb_api_v3") ->
    ?DESC("influxdb_api_v3").

%% -------------------------------------------------------------------------------------------------
%% internal functions

start_client(InstId, Config) ->
    ClientConfig = client_config(InstId, Config),
    ?SLOG(info, #{
        msg => "starting_influxdb_connector",
        connector => InstId,
        config => emqx_utils:redact(Config),
        client_config => emqx_utils:redact(ClientConfig)
    }),
    try do_start_client(InstId, ClientConfig, Config) of
        Res = {ok, #{client := Client}} ->
            ok = emqx_resource:allocate_resource(InstId, ?MODULE, ?influx_client, Client),
            Res;
        {error, Reason} ->
            {error, Reason}
    catch
        E:R:S ->
            ?tp(influxdb_connector_start_exception, #{error => {E, R}}),
            ?SLOG(warning, #{
                msg => "start_influxdb_connector_error",
                connector => InstId,
                error => E,
                reason => R,
                stack => S
            }),
            {error, R}
    end.

do_start_client(InstId, ClientConfig, Config) ->
    case influxdb:start_client(ClientConfig) of
        {ok, Client} ->
            case influxdb:is_alive(Client, true) of
                true ->
                    case influxdb:check_auth(Client) of
                        ok ->
                            State = #{client => Client, channels => #{}},
                            ?SLOG(info, #{
                                msg => "starting_influxdb_connector_success",
                                connector => InstId,
                                client => redact_auth(Client),
                                state => redact_auth(State)
                            }),
                            {ok, State};
                        Error ->
                            ?tp(influxdb_connector_start_failed, #{reason => auth_error}),
                            ?SLOG(warning, #{
                                msg => "failed_to_start_influxdb_connector",
                                error => Error,
                                connector => InstId,
                                client => redact_auth(Client),
                                reason => auth_error
                            }),
                            %% no leak
                            _ = influxdb:stop_client(Client),
                            {error, connect_ok_but_auth_failed}
                    end;
                {false, Reason0} ->
                    Reason =
                        case Reason0 of
                            401 -> auth_error;
                            _ -> Reason0
                        end,
                    ?tp(influxdb_connector_start_failed, #{
                        error => influxdb_client_not_alive,
                        reason => Reason,
                        raw_reason => Reason0
                    }),
                    ?SLOG(warning, #{
                        msg => "failed_to_start_influxdb_connector",
                        connector => InstId,
                        client => redact_auth(Client),
                        reason => Reason
                    }),
                    %% no leak
                    _ = influxdb:stop_client(Client),
                    {error, {connect_failed, Reason}}
            end;
        {error, {already_started, Client0}} ->
            ?tp(influxdb_connector_start_already_started, #{}),
            ?SLOG(info, #{
                msg => "restarting_influxdb_connector_found_already_started_client",
                connector => InstId,
                old_client => redact_auth(Client0)
            }),
            _ = influxdb:stop_client(Client0),
            do_start_client(InstId, ClientConfig, Config);
        {error, Reason} ->
            ?tp(influxdb_connector_start_failed, #{error => Reason}),
            ?SLOG(warning, #{
                msg => "failed_to_start_influxdb_connector",
                connector => InstId,
                reason => Reason
            }),
            {error, Reason}
    end.

client_config(
    InstId,
    Config = #{
        server := Server
    }
) ->
    #{hostname := Host, port := Port} = emqx_schema:parse_server(Server, ?INFLUXDB_HOST_OPTIONS),
    [
        {host, str(Host)},
        {port, Port},
        {pool_size, maps:get(pool_size, Config, ?DEFAULT_POOL_SIZE)},
        {pool, InstId}
    ] ++ protocol_config(Config).

%% api v1 config
protocol_config(#{
    parameters := #{influxdb_type := influxdb_api_v1, database := DB} = Params, ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v1},
        {database, str(DB)}
    ] ++ username(Params) ++ password(Params) ++ ping_with_auth(Params) ++ ssl_config(SSL) ++
        v1_auth_transport(Params);
%% api v2 config
protocol_config(#{
    parameters :=
        #{influxdb_type := influxdb_api_v2, bucket := Bucket, org := Org, token := Token} = Params,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v2},
        {bucket, str(Bucket)},
        {org, str(Org)},
        %% TODO: teach `influxdb` to accept 0-arity closures as passwords.
        {token, emqx_secret:unwrap(Token)}
    ] ++ ping_with_auth(Params) ++ ssl_config(SSL);
protocol_config(#{
    parameters := #{influxdb_type := influxdb_api_v3, database := Database, token := Token} =
        Params,
    ssl := SSL
}) ->
    [
        {protocol, http},
        {version, v3},
        {database, Database},
        %% TODO: teach `influxdb` to accept 0-arity closures as passwords.
        {token, emqx_secret:unwrap(Token)}
    ] ++ ping_with_auth(Params) ++ ssl_config(SSL).

v1_auth_transport(#{v1_auth_transport := V1AuthTransport}) ->
    [{v1_auth_transport, V1AuthTransport}];
v1_auth_transport(_) ->
    [].

ssl_config(#{enable := false}) ->
    [
        {https_enabled, false}
    ];
ssl_config(SSL = #{enable := true}) ->
    [
        {https_enabled, true},
        {transport, ssl},
        {transport_opts, emqx_tls_lib:to_client_opts(SSL)}
    ].

username(#{username := Username}) ->
    [{username, str(Username)}];
username(_) ->
    [].

password(#{password := Password}) ->
    %% TODO: teach `influxdb` to accept 0-arity closures as passwords.
    [{password, str(emqx_secret:unwrap(Password))}];
password(_) ->
    [].

ping_with_auth(#{ping_with_auth := PingWithAuth}) ->
    [{ping_with_auth, PingWithAuth}];
ping_with_auth(_) ->
    [].

redact_auth(Term) ->
    emqx_utils:redact(Term, fun is_auth_key/1).

is_auth_key(path) ->
    true;
is_auth_key(auth_path) ->
    true;
is_auth_key(Key) when is_binary(Key) ->
    string:equal("authorization", Key, true);
is_auth_key(_) ->
    false.

%% -------------------------------------------------------------------------------------------------
%% Query
do_query(_InstId, _Channel, _Client, []) ->
    ok;
do_query(InstId, Channel, Client, Points) ->
    emqx_trace:rendered_action_template(
        Channel, #{points => Points, is_async => false}
    ),
    case influxdb:write(Client, Points) of
        ok ->
            ?SLOG(debug, #{
                msg => "influxdb_write_point_success",
                connector => InstId,
                points => Points
            });
        {error, {401, _, _}} ->
            ?tp(influxdb_connector_do_query_failure, #{error => <<"authorization failure">>}),
            ?SLOG(error, #{
                msg => "influxdb_authorization_failed",
                client => redact_auth(Client),
                connector => InstId
            }),
            {error, {unrecoverable_error, <<"authorization failure">>}};
        {error, Reason} = Err ->
            ?tp(influxdb_connector_do_query_failure, #{error => Reason}),
            ?SLOG(error, #{
                msg => "influxdb_write_point_failed",
                connector => InstId,
                reason => Reason
            }),
            case is_unrecoverable_error(Err) of
                true ->
                    {error, {unrecoverable_error, Reason}};
                false ->
                    {error, {recoverable_error, Reason}}
            end
    end.

do_async_query(_InstId, _Channel, _Client, [], ReplyFunAndArgs) ->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, ok),
    ok;
do_async_query(InstId, Channel, Client, Points, ReplyFunAndArgs) ->
    ?SLOG(info, #{
        msg => "influxdb_write_point_async",
        connector => InstId,
        points => Points
    }),
    emqx_trace:rendered_action_template(Channel, #{points => Points, is_async => true}),
    WrappedReplyFunAndArgs = {fun ?MODULE:reply_callback/2, [ReplyFunAndArgs]},
    {ok, _WorkerPid} = influxdb:write_async(Client, Points, WrappedReplyFunAndArgs).

reply_callback(ReplyFunAndArgs, Result) ->
    emqx_resource:apply_reply_fun(ReplyFunAndArgs, classify_result(Result)).

classify_result({error, {unrecoverable_error, _}} = Error) ->
    %% already classified, e.g. a parse failure reported by the coordinated
    %% batch callback; do not wrap it again
    Error;
classify_result({error, {recoverable_error, _}} = Error) ->
    Error;
classify_result({error, Reason} = Error) ->
    case is_unrecoverable_error(Error) of
        true ->
            {error, {unrecoverable_error, Reason}};
        false ->
            {error, {recoverable_error, Reason}}
    end;
classify_result({ok, 401, _, _}) ->
    ?tp(influxdb_connector_do_query_failure, #{error => <<"authorization failure">>}),
    {error, {unrecoverable_error, <<"authorization failure">>}};
classify_result({ok, 401, _}) ->
    ?tp(influxdb_connector_do_query_failure, #{error => <<"authorization failure">>}),
    {error, {unrecoverable_error, <<"authorization failure">>}};
classify_result({ok, Code, _, Body}) when ?IS_HTTP_ERROR(Code) ->
    Error = #{code => Code, body => Body},
    ?tp(influxdb_connector_do_query_failure, #{error => Error}),
    {error, {unrecoverable_error, Error}};
classify_result({ok, Code, _}) when ?IS_HTTP_ERROR(Code) ->
    Error = #{code => Code, body => <<"">>},
    ?tp(influxdb_connector_do_query_failure, #{error => Error}),
    {error, {unrecoverable_error, Error}};
classify_result(Result) ->
    ?tp(influxdb_connector_do_query_ok, #{result => Result}),
    Result.

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Config Trans

write_config(#{write := Points}) ->
    {structured, Points};
write_config(#{write_syntax := Lines}) ->
    {legacy, Lines}.

to_config({legacy, Lines}, Precision) ->
    legacy_to_config(Lines, [], Precision);
to_config({structured, Points}, Precision) ->
    [structured_point_to_config(Point, Precision) || Point <- Points].

legacy_to_config([], Acc, _Precision) ->
    lists:reverse(Acc);
legacy_to_config([#{line := Line0} | Rest], Acc, Precision) ->
    legacy_to_config(Rest, [#{line => emqx_placeholder:preproc_tmpl(Line0)} | Acc], Precision);
legacy_to_config([Item0 | Rest], Acc, Precision) ->
    Ts0 = maps:get(timestamp, Item0, undefined),
    {Ts, FromPrecision, ToPrecision} = preproc_tmpl_timestamp(Ts0, Precision),
    Item = #{
        measurement => emqx_placeholder:preproc_tmpl(maps:get(measurement, Item0)),
        timestamp => Ts,
        precision => {FromPrecision, ToPrecision},
        tags => to_kv_config(maps:get(tags, Item0)),
        fields => to_kv_config(maps:get(fields, Item0))
    },
    legacy_to_config(Rest, [Item | Acc], Precision).

structured_point_to_config(Point, TargetPrecision) ->
    {Timestamp, SourcePrecision, MissingTimestamp} = structured_timestamp(
        maps:get(timestamp, Point, undefined), TargetPrecision
    ),
    #{
        measurement => {structured, emqx_placeholder:preproc_tmpl(maps:get(measurement, Point))},
        tags => structured_tags(maps:get(tags, Point, [])),
        fields => structured_fields(maps:get(fields, Point)),
        timestamp => Timestamp,
        missing_timestamp => MissingTimestamp,
        precision => {SourcePrecision, TargetPrecision}
    }.

structured_timestamp(false, TargetPrecision) ->
    {undefined, TargetPrecision, ignore};
structured_timestamp(undefined, TargetPrecision) ->
    {Ts, SourcePrecision, _} = preproc_tmpl_timestamp(undefined, TargetPrecision),
    {Ts, SourcePrecision, default};
structured_timestamp(#{value := Value, source_precision := SourcePrecision}, _TargetPrecision) ->
    {emqx_placeholder:preproc_tmpl(Value), SourcePrecision, error}.

structured_tags(Tags) ->
    lists:foldl(
        fun(#{key := Key, value := Value}, Acc) ->
            maps:put(
                emqx_placeholder:preproc_tmpl(Key),
                {structured_tag, emqx_placeholder:preproc_tmpl(Value)},
                Acc
            )
        end,
        #{},
        Tags
    ).

structured_fields(Fields) ->
    lists:foldl(
        fun(#{key := Key, type := Type, value := Value}, Acc) ->
            maps:put(
                emqx_placeholder:preproc_tmpl(Key),
                {typed, Type, emqx_placeholder:preproc_tmpl(Value)},
                Acc
            )
        end,
        #{},
        Fields
    ).

%% pre-process the timestamp template
%% returns a tuple of three elements:
%% 1. The timestamp template itself.
%% 2. The source timestamp precision (ms if the template ${timestamp} is used).
%% 3. The target timestamp precision (configured for the client).
preproc_tmpl_timestamp(undefined, Precision) ->
    %% not configured, we default it to the message timestamp
    preproc_tmpl_timestamp(?DEFAULT_TIMESTAMP_TMPL, Precision);
preproc_tmpl_timestamp(Ts, Precision) when is_integer(Ts) ->
    %% a const value is used which is very much unusual, but we have to add a special handling
    {Ts, Precision, Precision};
preproc_tmpl_timestamp(Ts, Precision) when is_list(Ts) ->
    preproc_tmpl_timestamp(iolist_to_binary(Ts), Precision);
preproc_tmpl_timestamp(<<?DEFAULT_TIMESTAMP_TMPL>> = Ts, Precision) ->
    {emqx_placeholder:preproc_tmpl(Ts), ms, Precision};
preproc_tmpl_timestamp(Ts, Precision) when is_binary(Ts) ->
    %% a placehold is in use. e.g. ${payload.my_timestamp}
    %% we can only hope it the value will be of the same precision in the configs
    {emqx_placeholder:preproc_tmpl(Ts), Precision, Precision}.

to_kv_config(KVfields) ->
    maps:fold(fun to_maps_config/3, #{}, proplists:to_map(KVfields)).

to_maps_config(K, V, Res) ->
    NK = emqx_placeholder:preproc_tmpl(bin(K)),
    Res#{NK => preproc_quoted(V)}.

preproc_quoted({quoted, V}) ->
    {quoted, emqx_placeholder:preproc_tmpl(bin(V))};
preproc_quoted(V) ->
    emqx_placeholder:preproc_tmpl(bin(V)).

proc_quoted({quoted, V}, Data, TransOpts) ->
    {quoted, emqx_placeholder:proc_tmpl(V, Data, TransOpts)};
proc_quoted(V, Data, TransOpts) ->
    emqx_placeholder:proc_tmpl(V, Data, TransOpts).

%% -------------------------------------------------------------------------------------------------
%% Tags & Fields Data Trans
parse_batch_data(InstId, BatchData, SyntaxLines) ->
    {Points, Errors} = lists:foldl(
        fun({_, Data}, {ListOfPoints, ErrAccIn}) ->
            case data_to_points(Data, SyntaxLines) of
                {ok, Points} ->
                    {[Points | ListOfPoints], ErrAccIn};
                {error, ErrorPoints} ->
                    log_error_points(InstId, ErrorPoints),
                    {ListOfPoints, ErrAccIn + 1}
            end
        end,
        {[], 0},
        BatchData
    ),
    case Errors of
        0 ->
            {ok, lists:flatten(Points)};
        _ ->
            ?SLOG(error, #{
                msg => "influxdb_trans_point_failed",
                error_count => Errors,
                connector => InstId,
                reason => points_trans_failed
            }),
            {error, points_trans_failed}
    end.

-spec data_to_points(map(), [
    #{
        fields := [{binary(), binary()}],
        measurement := binary(),
        tags := [{binary(), binary()}],
        timestamp := emqx_placeholder:tmpl_token() | integer(),
        precision := {From :: ts_precision(), To :: ts_precision()}
    }
    | #{line := emqx_placeholder:tmpl_token()}
]) -> {ok, [map() | binary()]} | {error, term()}.
data_to_points(Data, SyntaxLines) ->
    lines_to_points(Data, SyntaxLines, [], []).

%% When converting multiple rows data into InfluxDB Line Protocol, they are considered to be strongly correlated.
%% And once a row fails to convert, all of them are considered to have failed.
lines_to_points(_, [], Points, ErrorPoints) ->
    case ErrorPoints of
        [] ->
            {ok, Points};
        _ ->
            %% ignore trans succeeded points
            {error, ErrorPoints}
    end;
%% a raw line protocol template, the rendered value is written verbatim
lines_to_points(Data, [#{line := Tmpl} | Rest], PointsAcc, ErrorPointsAcc) ->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    case emqx_placeholder:proc_tmpl(Tmpl, Data, TransOptions) of
        [undefined] ->
            lines_to_points(Data, Rest, PointsAcc, ErrorPointsAcc);
        [RawLine0] ->
            case raw_line_to_bin(RawLine0) of
                {ok, RawLine1} ->
                    RawLine = string:trim(RawLine1, trailing, "\n"),
                    case RawLine of
                        <<>> ->
                            lines_to_points(Data, Rest, PointsAcc, ErrorPointsAcc);
                        _ ->
                            lines_to_points(Data, Rest, [RawLine | PointsAcc], ErrorPointsAcc)
                    end;
                {error, BadRawLine} ->
                    lines_to_points(Data, Rest, PointsAcc, [
                        {error, {non_textual_raw_line, BadRawLine}} | ErrorPointsAcc
                    ])
            end;
        _ ->
            lines_to_points(Data, Rest, PointsAcc, ErrorPointsAcc)
    end;
lines_to_points(
    Data,
    [#{timestamp := Ts, precision := Precision} = Item | Rest],
    ResultPointsAcc,
    ErrorPointsAcc
) when
    is_list(Ts)
->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    MissingTimestamp = maps:get(missing_timestamp, Item, default),
    case
        parse_timestamp(
            emqx_placeholder:proc_tmpl(Ts, Data, TransOptions),
            Precision,
            MissingTimestamp
        )
    of
        {ok, TsInt} ->
            Item1 = Item#{timestamp => TsInt},
            continue_lines_to_points(Data, Item1, Rest, ResultPointsAcc, ErrorPointsAcc);
        {error, BadTs} ->
            lines_to_points(Data, Rest, ResultPointsAcc, [
                {error, {bad_timestamp, BadTs}} | ErrorPointsAcc
            ])
    end;
lines_to_points(
    Data,
    [#{timestamp := Ts, precision := Precision} = Item | Rest],
    ResultPointsAcc,
    ErrorPointsAcc
) when
    is_integer(Ts)
->
    continue_lines_to_points(
        Data,
        Item#{timestamp => maybe_convert_time_unit(Ts, Precision)},
        Rest,
        ResultPointsAcc,
        ErrorPointsAcc
    );
lines_to_points(
    Data,
    [#{timestamp := undefined} = Item | Rest],
    ResultPointsAcc,
    ErrorPointsAcc
) ->
    continue_lines_to_points(Data, Item, Rest, ResultPointsAcc, ErrorPointsAcc).

parse_timestamp([undefined], _Precision, error) ->
    {error, missing_timestamp};
parse_timestamp([undefined], {_From, To} = _Precision, default) ->
    %% used ${timestamp} or keep it blank. but the `timestamp` field not present in RULE SQL
    {ok, maybe_convert_time_unit(emqx_message:timestamp_now(), {ms, To})};
parse_timestamp([TsInt], Precision, _MissingTimestamp) when is_integer(TsInt) ->
    {ok, maybe_convert_time_unit(TsInt, Precision)};
parse_timestamp([TsBin], Precision, _MissingTimestamp) ->
    try
        {ok, maybe_convert_time_unit(binary_to_integer(TsBin), Precision)}
    catch
        _:_ ->
            {error, {non_integer_timestamp, TsBin}}
    end;
parse_timestamp(InvalidTs, _, _MissingTimestamp) ->
    %% The timestamp field must be a single integer or a single placeholder. i.e. the
    %%   following is not allowed:
    %%   - weather,location=us-midwest,season=summer temperature=82 ${timestamp}00
    {error, {unsupported_placeholder_usage_for_timestamp, InvalidTs}}.

maybe_convert_time_unit(Ts, {FromPrecision, ToPrecision}) ->
    erlang:convert_time_unit(Ts, time_unit(FromPrecision), time_unit(ToPrecision)).

time_unit(s) -> second;
time_unit(ms) -> millisecond;
time_unit(us) -> microsecond;
time_unit(ns) -> nanosecond.

continue_lines_to_points(Data, Item, Rest, ResultPointsAcc, ErrorPointsAcc) ->
    case line_to_point(Data, Item) of
        #{fields := Fields} when map_size(Fields) =:= 0 ->
            %% influxdb client doesn't like empty field maps...
            ErrorPointsAcc1 = [{error, no_fields} | ErrorPointsAcc],
            lines_to_points(Data, Rest, ResultPointsAcc, ErrorPointsAcc1);
        {error, Reason} ->
            ErrorPointsAcc1 = [{error, Reason} | ErrorPointsAcc],
            lines_to_points(Data, Rest, ResultPointsAcc, ErrorPointsAcc1);
        Point ->
            lines_to_points(Data, Rest, [Point | ResultPointsAcc], ErrorPointsAcc)
    end.

line_to_point(
    Data,
    Item
) ->
    try line_to_point_1(Data, Item) of
        Point -> Point
    catch
        throw:Reason ->
            {error, Reason}
    end.

line_to_point_1(
    Data,
    #{
        measurement := Measurement,
        tags := Tags,
        fields := Fields,
        timestamp := Ts
    } = Item
) ->
    {_, EncodedTags, _} = maps:fold(fun maps_config_to_data/3, {Data, #{}, ?set_tag}, Tags),
    {_, EncodedFields, _} = maps:fold(fun maps_config_to_data/3, {Data, #{}, ?set_field}, Fields),
    maps:without([precision, missing_timestamp], Item#{
        measurement => render_measurement(Measurement, Data),
        tags => EncodedTags,
        fields => EncodedFields,
        timestamp => Ts
    }).

render_measurement({structured, Tmpl}, Data) ->
    TransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    Parts = emqx_placeholder:proc_tmpl(Tmpl, Data, TransOptions),
    case {lists:member(undefined, Parts), render_template_text(Parts)} of
        {true, _} ->
            throw(missing_measurement);
        {false, <<>>} ->
            throw(empty_measurement);
        {false, Measurement} ->
            Measurement
    end;
render_measurement(Tmpl, Data) ->
    emqx_placeholder:proc_tmpl(Tmpl, Data).

maps_config_to_data(K, {structured_tag, Tmpl}, {Data, Res, ?set_tag}) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = emqx_placeholder:proc_tmpl(Tmpl, Data, VTransOptions),
    case {has_undefined(NK), has_undefined(NV)} of
        {true, _} ->
            {Data, Res, ?set_tag};
        {_, true} ->
            {Data, Res, ?set_tag};
        _ ->
            NRes = put_structured_kv(
                tag_key,
                render_template_text(NK),
                render_template_text(NV),
                Res
            ),
            {Data, NRes, ?set_tag}
    end;
maps_config_to_data(K, {typed, Type, Tmpl}, {Data, Res, _SetType}) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = emqx_placeholder:proc_tmpl(Tmpl, Data, VTransOptions),
    case {has_undefined(NK), has_undefined(NV)} of
        {true, _} ->
            {Data, Res, ?set_field};
        {_, true} ->
            {Data, Res, ?set_field};
        _ ->
            Value =
                try
                    case Type of
                        auto ->
                            auto_field_value(NV, Tmpl);
                        _ ->
                            typed_field_value(Type, NV)
                    end
                catch
                    _:_ ->
                        throw({invalid_field_value, #{type => Type, value => NV}})
                end,
            NRes = put_structured_kv(
                field_key,
                render_template_text(NK),
                Value,
                Res
            ),
            {Data, NRes, ?set_field}
    end;
maps_config_to_data(K, V, {Data, Res, SetType}) ->
    KTransOptions = #{return => rawlist, var_trans => fun key_filter/1},
    VTransOptions = #{return => rawlist, var_trans => fun data_filter/1},
    NK = emqx_placeholder:proc_tmpl(K, Data, KTransOptions),
    NV = proc_quoted(V, Data, VTransOptions),
    case {NK, NV} of
        {[undefined], _} ->
            {Data, Res, SetType};
        %% undefined value in normal format [undefined] or int/uint format [undefined, <<"i">>]
        {_, [undefined | _]} ->
            {Data, Res, SetType};
        {_, {quoted, [undefined | _]}} ->
            {Data, Res, SetType};
        _ ->
            NRes = Res#{
                list_to_binary(NK) => value_type(NV, #{
                    tmpl_type => tmpl_type(V), set_type => SetType
                })
            },
            {Data, NRes, SetType}
    end.

typed_field_value(float, [Value]) when is_number(Value) ->
    {float, Value};
typed_field_value(float, [Value]) when is_binary(Value) ->
    ok = valid_float_binary(Value),
    {float, Value};
typed_field_value(integer, [Value]) when
    is_integer(Value), Value >= ?MIN_INT64, Value =< ?MAX_INT64
->
    {int, Value};
typed_field_value(integer, [Value]) when is_binary(Value) ->
    Int = binary_to_integer(Value),
    typed_field_value(integer, [Int]);
typed_field_value(uint, [Value]) when
    is_integer(Value), Value >= 0, Value =< ?MAX_UINT64
->
    {uint, Value};
typed_field_value(uint, [Value]) when is_binary(Value) ->
    UInt = binary_to_integer(Value),
    typed_field_value(uint, [UInt]);
typed_field_value(boolean, [Value]) when is_boolean(Value) ->
    Value;
typed_field_value(boolean, [Value]) when is_binary(Value) ->
    typed_boolean(Value);
typed_field_value(string, Values) when is_list(Values) ->
    {string, render_template_text(Values)}.

typed_boolean(Value) ->
    case string:lowercase(Value) of
        <<"true">> -> true;
        <<"t">> -> true;
        <<"false">> -> false;
        <<"f">> -> false
    end.

valid_float_binary(Value) ->
    try
        _ = binary_to_float(Value),
        ok
    catch
        error:badarg ->
            try
                _ = binary_to_integer(Value),
                ok
            catch
                error:badarg ->
                    case
                        re:run(
                            Value,
                            <<"^[+-]?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?$">>,
                            [{capture, none}]
                        )
                    of
                        match -> ok;
                        nomatch -> error(badarg)
                    end
            end
    end.

render_template_text(Values) when is_list(Values) ->
    iolist_to_binary([bin(Value) || Value <- Values]).

has_undefined(Values) ->
    lists:member(undefined, Values).

put_structured_kv(KeyType, Key, Value, Acc) ->
    case {Key, maps:is_key(Key, Acc)} of
        {<<>>, _} ->
            throw({empty_rendered_key, KeyType});
        {_, true} ->
            throw({duplicate_rendered_key, #{type => KeyType, key => Key}});
        {_, false} ->
            Acc#{Key => Value}
    end.

value_type({quoted, ValList}, _) ->
    {string_list, ValList};
value_type(Values, #{set_type := ?set_tag}) when is_list(Values) ->
    %% Tags are strings in InfluxDB line protocol, including values that end
    %% with i or u.
    render_template_text(Values);
value_type([Int, <<"i">>], #{set_type := ?set_field, tmpl_type := mixed}) when is_integer(Int) ->
    checked_auto_integer(int, Int);
value_type([UInt, <<"u">>], #{set_type := ?set_field, tmpl_type := mixed}) when is_integer(UInt) ->
    checked_auto_integer(uint, UInt);
value_type([Invalid, <<"i">>], #{set_type := ?set_field}) ->
    throw({invalid_integer_value, Invalid});
value_type([Invalid, <<"u">>], #{set_type := ?set_field}) ->
    throw({invalid_unsigned_integer_value, Invalid});
%% write `1`, `1.0`, `-1.0` all as float
%% see also: https://docs.influxdata.com/influxdb/v2.7/reference/syntax/line-protocol/#float
value_type([Number], #{set_type := ?set_field}) when is_number(Number) ->
    {float, Number};
value_type([<<"t">>], _) ->
    't';
value_type([<<"T">>], _) ->
    'T';
value_type([true], _) ->
    'true';
value_type([<<"TRUE">>], _) ->
    'TRUE';
value_type([<<"True">>], _) ->
    'True';
value_type([<<"f">>], _) ->
    'f';
value_type([<<"F">>], _) ->
    'F';
value_type([false], _) ->
    'false';
value_type([<<"FALSE">>], _) ->
    'FALSE';
value_type([<<"False">>], _) ->
    'False';
value_type([Str], #{tmpl_type := variable}) when is_binary(Str) ->
    Str;
value_type([Str], #{tmpl_type := literal, set_type := ?set_field}) when is_binary(Str) ->
    %% if Str is a literal string suffixed with `i` or `u`, we should convert it to int/uint.
    %% otherwise, we should convert it to float.
    NumStr = binary:part(Str, 0, byte_size(Str) - 1),
    case binary:part(Str, byte_size(Str), -1) of
        <<"i">> ->
            maybe_convert_to_integer(NumStr, Str, int);
        <<"u">> ->
            maybe_convert_to_integer(NumStr, Str, uint);
        _ ->
            maybe_convert_to_float_str(Str)
    end;
value_type(Str, _) ->
    Str.

auto_field_value(Values, Tmpl) ->
    TemplateType = tmpl_type(Tmpl),
    Options = #{
        tmpl_type => TemplateType,
        set_type => ?set_field
    },
    case TemplateType of
        mixed ->
            case is_auto_integer_suffix_template(Tmpl) of
                true -> value_type(Values, Options);
                false -> render_template_text(Values)
            end;
        _ ->
            value_type(Values, Options)
    end.

is_auto_integer_suffix_template([{var, _}, {str, <<"i">>}]) ->
    true;
is_auto_integer_suffix_template([{var, _}, {str, <<"u">>}]) ->
    true;
is_auto_integer_suffix_template(_) ->
    false.

tmpl_type([{str, _}]) ->
    literal;
tmpl_type([{var, _}]) ->
    variable;
tmpl_type(_) ->
    mixed.

maybe_convert_to_integer(NumStr, String, Type) ->
    try
        Int = binary_to_integer(NumStr),
        checked_auto_integer(Type, Int)
    catch
        error:badarg ->
            maybe_convert_to_integer_f(NumStr, String, Type)
    end.

maybe_convert_to_integer_f(NumStr, String, Type) ->
    try
        Float = binary_to_float(NumStr),
        Int = erlang:floor(Float),
        case Float == Int of
            true -> checked_auto_integer(Type, Int);
            false -> throw({invalid_integer_value, String})
        end
    catch
        error:badarg ->
            String
    end.

checked_auto_integer(int, Value) when
    Value >= ?MIN_INT64, Value =< ?MAX_INT64
->
    {int, Value};
checked_auto_integer(int, Value) ->
    throw({invalid_integer_value, Value});
checked_auto_integer(uint, Value) when
    Value >= 0, Value =< ?MAX_UINT64
->
    {uint, Value};
checked_auto_integer(uint, Value) ->
    throw({invalid_unsigned_integer_value, Value}).

maybe_convert_to_float_str(NumStr) ->
    try
        _ = binary_to_float(NumStr),
        %% NOTE: return a {float, String} to avoid precision loss when converting to float
        {float, NumStr}
    catch
        error:badarg ->
            maybe_convert_to_float_str_i(NumStr)
    end.

maybe_convert_to_float_str_i(NumStr) ->
    try
        _ = binary_to_integer(NumStr),
        {float, NumStr}
    catch
        error:badarg ->
            NumStr
    end.

key_filter(undefined) -> undefined;
key_filter(Value) -> bin(Value).

data_filter(undefined) -> undefined;
data_filter(Int) when is_integer(Int) -> Int;
data_filter(Number) when is_number(Number) -> Number;
data_filter(Bool) when is_boolean(Bool) -> Bool;
data_filter(Data) -> bin(Data).

%% a raw line protocol line must be actual text; a value that cannot be
%% text (a number, boolean, ...) is a conversion error, so the write is not
%% silently turned into a no-op.
raw_line_to_bin(<<_/binary>> = Raw) ->
    {ok, Raw};
raw_line_to_bin(Raw) when is_list(Raw) ->
    {ok, bin(Raw)};
raw_line_to_bin(Raw) ->
    {error, Raw}.

bin(Data) when is_list(Data) ->
    case io_lib:printable_unicode_list(Data) of
        true -> unicode:characters_to_binary(Data);
        false -> emqx_utils_conv:bin(Data)
    end;
bin(Data) ->
    emqx_utils_conv:bin(Data).

%% helper funcs
log_error_points(InstId, Errs) ->
    lists:foreach(
        fun({error, Reason}) ->
            ?SLOG(error, #{
                msg => "influxdb_trans_point_failed",
                connector => InstId,
                reason => Reason
            })
        end,
        Errs
    ).

convert_server(<<"http://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(<<"https://", Server/binary>>, HoconOpts) ->
    convert_server(Server, HoconOpts);
convert_server(Server0, HoconOpts) ->
    Server = string:trim(Server0, trailing, "/"),
    emqx_schema:convert_servers(Server, HoconOpts).

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.

is_unrecoverable_error({error, {unrecoverable_error, _}}) ->
    true;
is_unrecoverable_error({error, {Code, _}}) when ?IS_HTTP_ERROR(Code) ->
    true;
is_unrecoverable_error({error, {Code, _, _Body}}) when ?IS_HTTP_ERROR(Code) ->
    true;
is_unrecoverable_error(_) ->
    false.

%%===================================================================
%% eunit tests
%%===================================================================

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

structured_write_callback_test() ->
    ok = meck:new(influxdb, [passthrough, no_link]),
    try
        ok = meck:expect(influxdb, update_precision, fun(Client, _Precision) -> Client end),
        ok = meck:expect(
            influxdb,
            write,
            fun(_Client, Points) ->
                self() ! {structured_write_points, Points},
                ok
            end
        ),
        Client = #{
            protocol => http,
            opts => [{version, v2}, {precision, ms}],
            path => "/api/v2/write"
        },
        State0 = #{client => Client, channels => #{}},
        ChannelConfig = #{
            parameters => #{
                precision => ns,
                write => [
                    #{
                        measurement => <<"${topic}">>,
                        tags => [
                            #{key => <<"clientid">>, value => <<"${clientid}">>}
                        ],
                        fields => [
                            #{
                                key => <<"temperature">>,
                                type => float,
                                value => <<"${payload.temperature}">>
                            },
                            #{
                                key => <<"count">>,
                                type => integer,
                                value => <<"${payload.count}">>
                            },
                            #{
                                key => <<"unsigned">>,
                                type => uint,
                                value => <<"${payload.unsigned}">>
                            },
                            #{
                                key => <<"online">>,
                                type => boolean,
                                value => <<"${payload.online}">>
                            }
                        ],
                        timestamp => #{
                            value => <<"${payload.ts}">>,
                            source_precision => ms
                        }
                    }
                ]
            }
        },
        {ok, State} = on_add_channel(test_instance, State0, test_channel, ChannelConfig),
        Data = #{
            topic => <<"sensors">>,
            clientid => <<"client-1">>,
            payload => #{
                temperature => 21.5,
                count => 2,
                unsigned => 3,
                online => true,
                ts => 1700000000123
            }
        },
        ok = on_query(test_instance, {test_channel, Data}, State),
        receive
            {structured_write_points, [Point]} ->
                ?assertEqual(
                    #{
                        measurement => <<"sensors">>,
                        tags => #{<<"clientid">> => <<"client-1">>},
                        fields => #{
                            <<"temperature">> => {float, 21.5},
                            <<"count">> => {int, 2},
                            <<"unsigned">> => {uint, 3},
                            <<"online">> => true
                        },
                        timestamp => 1700000000123000000
                    },
                    Point
                )
        after 1000 ->
            error(no_structured_write)
        end
    after
        meck:unload(influxdb)
    end.

legacy_write_syntax_callback_test() ->
    ok = meck:new(influxdb, [passthrough, no_link]),
    try
        ok = meck:expect(influxdb, update_precision, fun(Client, _Precision) -> Client end),
        ok = meck:expect(
            influxdb,
            write,
            fun(_Client, Points) ->
                self() ! {legacy_write_points, Points},
                ok
            end
        ),
        Client = #{
            protocol => http,
            opts => [{version, v2}, {precision, ms}],
            path => "/api/v2/write"
        },
        State0 = #{client => Client, channels => #{}},
        ChannelConfig = #{
            parameters => #{
                write_syntax => emqx_bridge_influxdb:to_influx_lines(
                    <<"m,clientid=${clientid} value=${payload.value}i">>
                )
            }
        },
        {ok, State} = on_add_channel(test_instance, State0, test_channel, ChannelConfig),
        Data = #{clientid => <<"client-1">>, payload => #{value => 7}},
        ok = on_query(test_instance, {test_channel, Data}, State),
        receive
            {legacy_write_points, [Point]} ->
                ?assertEqual(<<"m">>, maps:get(measurement, Point)),
                ?assertEqual(#{<<"clientid">> => <<"client-1">>}, maps:get(tags, Point)),
                ?assertEqual(#{<<"value">> => {int, 7}}, maps:get(fields, Point))
        after 1000 ->
            error(no_legacy_write)
        end
    after
        meck:unload(influxdb)
    end.

structured_field_value_coercion_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            tags => [],
            fields => [
                #{key => <<"f">>, type => float, value => <<"${payload.f}">>},
                #{key => <<"i">>, type => integer, value => <<"${payload.i}">>},
                #{key => <<"u">>, type => uint, value => <<"${payload.u}">>},
                #{key => <<"b">>, type => boolean, value => <<"${payload.b}">>}
            ],
            timestamp => false
        }
    ],
    Data = #{
        payload => #{
            f => <<"21.5">>,
            i => <<"2">>,
            u => <<"3">>,
            b => <<"true">>
        }
    },
    {ok, [Point]} = run_structured_callback(Write, Data),
    ?assertEqual(
        #{
            <<"f">> => {float, <<"21.5">>},
            <<"i">> => {int, 2},
            <<"u">> => {uint, 3},
            <<"b">> => true
        },
        maps:get(fields, Point)
    ),
    ?assertEqual(undefined, maps:get(timestamp, Point)).

structured_multiple_points_single_write_test() ->
    Write = [
        #{
            measurement => <<"temperature">>,
            fields => [
                #{key => <<"value">>, type => float, value => <<"${payload.temperature}">>}
            ],
            timestamp => false
        },
        #{
            measurement => <<"humidity">>,
            fields => [
                #{key => <<"value">>, type => float, value => <<"${payload.humidity}">>}
            ],
            timestamp => false
        }
    ],
    Data = #{payload => #{temperature => 21.5, humidity => 48.0}},
    {ok, Points} = run_structured_callback(Write, Data),
    ?assertEqual(2, length(Points)),
    ?assertEqual(
        [<<"humidity">>, <<"temperature">>],
        lists:sort([maps:get(measurement, Point) || Point <- Points])
    ).

structured_batch_writes_all_points_once_test() ->
    Write = [
        #{
            measurement => <<"temperature">>,
            fields => [
                #{key => <<"value">>, type => float, value => <<"${payload.temperature}">>}
            ],
            timestamp => false
        },
        #{
            measurement => <<"humidity">>,
            fields => [
                #{key => <<"value">>, type => float, value => <<"${payload.humidity}">>}
            ],
            timestamp => false
        }
    ],
    ok = meck:new(influxdb, [passthrough, no_link]),
    try
        ok = meck:expect(influxdb, update_precision, fun(Client, _Precision) -> Client end),
        ok = meck:expect(
            influxdb,
            write,
            fun(_Client, Points) ->
                self() ! {structured_batch_write_points, Points},
                ok
            end
        ),
        Client = #{
            protocol => http,
            opts => [{version, v2}, {precision, ms}],
            path => "/api/v2/write"
        },
        State0 = #{client => Client, channels => #{}},
        ChannelConfig = #{parameters => #{write => Write}},
        {ok, State} = on_add_channel(test_instance, State0, test_channel, ChannelConfig),
        Batch = [
            {test_channel, #{payload => #{temperature => 20.0, humidity => 40.0}}},
            {test_channel, #{payload => #{temperature => 21.0, humidity => 41.0}}}
        ],
        ok = on_batch_query(test_instance, Batch, State),
        receive
            {structured_batch_write_points, Points} ->
                ?assertEqual(4, length(Points))
        after 1000 ->
            error(no_structured_batch_write)
        end
    after
        meck:unload(influxdb)
    end.

structured_batch_dynamic_bucket_groups_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"${payload.value}">>}
            ],
            timestamp => false
        }
    ],
    ok = meck:new(influxdb, [passthrough, no_link]),
    try
        ok = meck:expect(influxdb, update_precision, fun(Client, _Precision) -> Client end),
        ok = meck:expect(
            influxdb,
            update_bucket,
            fun(Client, Bucket) ->
                Client#{bucket => Bucket}
            end
        ),
        ok = meck:expect(
            influxdb,
            write,
            fun(Client, Points) ->
                self() ! {structured_bucket_write, maps:get(bucket, Client), Points},
                ok
            end
        ),
        Client = #{
            protocol => http,
            opts => [{version, v2}, {precision, ms}],
            path => "/api/v2/write"
        },
        State0 = #{client => Client, channels => #{}},
        ChannelConfig = #{
            parameters => #{
                bucket => <<"${payload.bucket}">>,
                write => Write
            }
        },
        {ok, State} = on_add_channel(test_instance, State0, test_channel, ChannelConfig),
        Batch = [
            {test_channel, #{payload => #{bucket => <<"b1">>, value => 1}}},
            {test_channel, #{payload => #{bucket => <<"b2">>, value => 2}}}
        ],
        ok = on_batch_query(test_instance, Batch, State),
        Writes = collect_structured_bucket_writes(2, []),
        ?assertEqual([<<"b1">>, <<"b2">>], lists:sort(maps:keys(maps:from_list(Writes))))
    after
        meck:unload(influxdb)
    end.

collect_structured_bucket_writes(0, Acc) ->
    Acc;
collect_structured_bucket_writes(Count, Acc) ->
    receive
        {structured_bucket_write, Bucket, Points} ->
            collect_structured_bucket_writes(Count - 1, [{Bucket, Points} | Acc])
    after 1000 ->
        error({missing_structured_bucket_writes, Count, Acc})
    end.

structured_float_accepts_integer_string_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"f">>, type => float, value => <<"${payload.f}">>}
            ],
            timestamp => false
        }
    ],
    {ok, [Point]} = run_structured_callback(Write, #{payload => #{f => <<"2">>}}),
    ?assertEqual(#{<<"f">> => {float, <<"2">>}}, maps:get(fields, Point)).

structured_float_accepts_scientific_notation_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"f">>, type => float, value => <<"${payload.f}">>}
            ],
            timestamp => false
        }
    ],
    {ok, [Point]} = run_structured_callback(Write, #{payload => #{f => <<"1e3">>}}),
    ?assertEqual(#{<<"f">> => {float, <<"1e3">>}}, maps:get(fields, Point)).

structured_string_and_missing_field_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{
                    key => <<"greeting">>,
                    type => string,
                    value => <<"hello ", "${payload.name}">>
                },
                #{
                    key => <<"missing">>,
                    type => float,
                    value => <<"${payload.missing}">>
                }
            ],
            timestamp => false
        }
    ],
    {ok, [Point]} = run_structured_callback(Write, #{payload => #{name => <<"emqx">>}}),
    ?assertEqual(
        #{<<"greeting">> => {string, <<"hello emqx">>}},
        maps:get(fields, Point)
    ).

structured_tag_mixed_template_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            tags => [
                #{
                    key => <<"device-", "${payload.kind}">>,
                    value => <<"tenant-", "${payload.tenant}">>
                }
            ],
            fields => [
                #{key => <<"value">>, type => integer, value => <<"${payload.value}">>}
            ],
            timestamp => false
        }
    ],
    Data = #{
        payload => #{
            kind => <<"sensor">>,
            tenant => <<"acme">>,
            value => 1
        }
    },
    {ok, [Point]} = run_structured_callback(Write, Data),
    ?assertEqual(
        #{<<"device-sensor">> => <<"tenant-acme">>},
        maps:get(tags, Point)
    ).

structured_auto_field_type_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"value">>, type => auto, value => <<"${payload.value}">>}
            ],
            timestamp => false
        }
    ],
    {ok, [Point]} = run_structured_callback(Write, #{payload => #{value => 1}}),
    ?assertEqual(#{<<"value">> => {float, 1}}, maps:get(fields, Point)).

structured_auto_mixed_template_is_text_test_() ->
    [
        ?_assertEqual(
            #{<<"value">> => <<"prefix65">>},
            structured_auto_mixed_template_field(65)
        ),
        ?_assertEqual(
            #{<<"value">> => <<"prefix300">>},
            structured_auto_mixed_template_field(300)
        ),
        ?_assertEqual(
            #{<<"value">> => <<"prefix1.5">>},
            structured_auto_mixed_template_field(1.5)
        ),
        ?_assertEqual(
            #{<<"value">> => <<"prefixtrue">>},
            structured_auto_mixed_template_field(true)
        )
    ].

structured_auto_mixed_template_field(Value) ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{
                    key => <<"value">>,
                    type => auto,
                    value => <<"prefix", "${payload.value}">>
                }
            ],
            timestamp => false
        }
    ],
    {ok, [Point]} = run_structured_callback(Write, #{payload => #{value => Value}}),
    maps:get(fields, Point).

structured_all_fields_missing_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"value">>, type => float, value => <<"${payload.value}">>}
            ],
            timestamp => false
        }
    ],
    assert_structured_write_error(Write, #{payload => #{}}).

structured_field_type_error_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{
                    key => <<"count">>,
                    type => integer,
                    value => <<"${payload.count}">>
                }
            ],
            timestamp => false
        }
    ],
    assert_structured_write_error(Write, #{payload => #{count => <<"1.5">>}}).

structured_uint_overflow_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"count">>, type => uint, value => <<"${payload.count}">>}
            ],
            timestamp => false
        }
    ],
    assert_structured_write_error(
        Write,
        #{payload => #{count => 18446744073709551616}}
    ).

structured_duplicate_rendered_field_key_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"${payload.k1}">>, type => integer, value => <<"1">>},
                #{key => <<"${payload.k2}">>, type => integer, value => <<"2">>}
            ],
            timestamp => false
        }
    ],
    assert_structured_write_error(
        Write,
        #{payload => #{k1 => <<"value">>, k2 => <<"value">>}}
    ).

structured_missing_measurement_test() ->
    Write = [
        #{
            measurement => <<"${payload.measurement}">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"${payload.value}">>}
            ],
            timestamp => false
        }
    ],
    assert_structured_write_error(Write, #{payload => #{value => 1}}).

structured_empty_field_key_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"${payload.key}">>, type => integer, value => <<"1">>}
            ],
            timestamp => false
        }
    ],
    assert_structured_write_error(Write, #{payload => #{key => <<>>}}).

structured_mixed_template_missing_value_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            tags => [
                #{key => <<"tenant">>, value => <<"tenant-", "${payload.missing}">>}
            ],
            fields => [
                #{
                    key => <<"greeting">>,
                    type => string,
                    value => <<"hello ", "${payload.missing}">>
                },
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => false
        }
    ],
    {ok, [Point]} = run_structured_callback(Write, #{payload => #{}}),
    ?assertEqual(#{}, maps:get(tags, Point)),
    ?assertEqual(#{<<"value">> => {int, 1}}, maps:get(fields, Point)).

structured_missing_timestamp_value_test() ->
    Write = [
        #{
            measurement => <<"m">>,
            fields => [
                #{key => <<"value">>, type => integer, value => <<"1">>}
            ],
            timestamp => #{
                value => <<"${payload.ts}">>,
                source_precision => ms
            }
        }
    ],
    assert_structured_write_error(Write, #{payload => #{}}).

trace_path_redacts_query_string_test() ->
    Client = #{path => "/write?u=user&p=secret&db=mqtt&precision=ms"},
    ?assertEqual("/write", trace_path(Client)).

grouped_async_trace_runs_before_spawn_test() ->
    PreviousMetadata = logger:get_process_metadata(),
    ok = logger:set_process_metadata(#{stop_action_after_render => true}),
    try
        Channel = <<"action:influxdb:test:connector:influxdb:c">>,
        ?assertException(
            error,
            {unrecoverable_error, {action_stopped_after_template_rendering, _}},
            trace_rendered_groups(Channel, [{client, [#{measurement => <<"m">>}]}], true)
        )
    after
        case PreviousMetadata of
            undefined -> logger:unset_process_metadata();
            _ -> logger:set_process_metadata(PreviousMetadata)
        end
    end.

assert_structured_write_error(Write, Data) ->
    ok = meck:new(influxdb, [passthrough, no_link]),
    try
        ok = meck:expect(influxdb, update_precision, fun(Client, _Precision) -> Client end),
        ok = meck:expect(
            influxdb,
            write,
            fun(_Client, _Points) ->
                error(unexpected_write)
            end
        ),
        Client = #{
            protocol => http,
            opts => [{version, v2}, {precision, ms}],
            path => "/api/v2/write"
        },
        State0 = #{client => Client, channels => #{}},
        ChannelConfig = #{parameters => #{write => Write}},
        {ok, State} = on_add_channel(test_instance, State0, test_channel, ChannelConfig),
        ?assertMatch(
            {error, {unrecoverable_error, _}},
            on_query(test_instance, {test_channel, Data}, State)
        )
    after
        meck:unload(influxdb)
    end.

run_structured_callback(Write, Data) ->
    ok = meck:new(influxdb, [passthrough, no_link]),
    try
        ok = meck:expect(influxdb, update_precision, fun(Client, _Precision) -> Client end),
        ok = meck:expect(
            influxdb,
            write,
            fun(_Client, Points) ->
                self() ! {structured_write_points, Points},
                ok
            end
        ),
        Client = #{
            protocol => http,
            opts => [{version, v2}, {precision, ms}],
            path => "/api/v2/write"
        },
        State0 = #{client => Client, channels => #{}},
        ChannelConfig = #{parameters => #{precision => ns, write => Write}},
        {ok, State} = on_add_channel(test_instance, State0, test_channel, ChannelConfig),
        ok = on_query(test_instance, {test_channel, Data}, State),
        receive
            {structured_write_points, Points} ->
                {ok, Points}
        after 1000 ->
            error(no_structured_write)
        end
    after
        meck:unload(influxdb)
    end.

is_auth_key_test_() ->
    [
        ?_assert(is_auth_key(<<"Authorization">>)),
        ?_assert(is_auth_key(path)),
        ?_assert(is_auth_key(auth_path)),
        ?_assertNot(is_auth_key(<<"Something">>)),
        ?_assertNot(is_auth_key(89))
    ].

redact_auth_test_() ->
    Secret = "public123",
    Client = #{
        path => "/write?p=" ++ Secret ++ "&u=admin&db=Datalayers",
        auth_path => "/query?p=" ++ Secret ++ "&u=admin&db=Datalayers"
    },
    Redacted = redact_auth(Client),
    Rendered = iolist_to_binary(io_lib:format("~p", [Redacted])),
    [
        ?_assertEqual(#{path => "******", auth_path => "******"}, Redacted),
        ?_assertEqual(nomatch, binary:match(Rendered, <<"public123">>))
    ].

render_bucket_test_() ->
    Tmpl = emqx_placeholder:preproc_tmpl(<<"${payload.bucket}">>),
    Data = #{payload => #{bucket => <<"mqtt2">>}},
    [
        {"renders the bucket from the data",
            ?_assertEqual({ok, <<"mqtt2">>}, render_bucket(Data, Tmpl))},
        {"no template renders undefined", ?_assertEqual(undefined, render_bucket(Data, undefined))},
        {"missing field falls back to the connector bucket",
            ?_assertEqual(undefined, render_bucket(#{payload => #{}}, Tmpl))},
        {"empty bucket falls back to the connector bucket",
            ?_assertEqual(
                undefined,
                render_bucket(#{payload => #{bucket => <<>>}}, Tmpl)
            )},
        {"bucket without template still overrides",
            ?_assertEqual(
                {ok, <<"fixed_bucket">>},
                render_bucket(Data, emqx_placeholder:preproc_tmpl(<<"fixed_bucket">>))
            )},
        {"static text and placeholder are concatenated",
            ?_assertEqual(
                {ok, <<"tenant-mqtt2">>},
                render_bucket(
                    Data,
                    emqx_placeholder:preproc_tmpl(<<"tenant-${payload.bucket}">>)
                )
            )},
        {"concatenated template with a missing part falls back",
            ?_assertEqual(
                undefined,
                render_bucket(
                    #{payload => #{}},
                    emqx_placeholder:preproc_tmpl(<<"tenant-${payload.bucket}">>)
                )
            )}
    ].

group_batch_by_bucket_test_() ->
    Tmpl = emqx_placeholder:preproc_tmpl(<<"${payload.bucket}">>),
    ChannelConfig = #{channel_client => client, bucket_tmpl => Tmpl},
    BatchData =
        [
            {c1, #{payload => #{bucket => Bucket}}}
         || Bucket <- [<<"b1">>, <<"b1">>, <<"b2">>, <<"b1">>, <<>>]
        ],
    {setup,
        fun() ->
            ok = meck:new(influxdb, [passthrough, no_link]),
            ok = meck:expect(influxdb, update_bucket, fun(_Client, Bucket) -> Bucket end),
            group_batch_by_bucket(BatchData, ChannelConfig)
        end,
        fun(_Groups) ->
            meck:unload(influxdb)
        end,
        fun(Groups) ->
            [
                {"the write path is rebuilt once per distinct bucket",
                    ?_assertEqual(2, meck:num_calls(influxdb, update_bucket, ['_', '_']))},
                {"batch items are grouped by rendered bucket; empty/missing falls back",
                    ?_assertEqual(
                        [{client, 1}, {<<"b1">>, 3}, {<<"b2">>, 1}],
                        lists:sort([{C, length(Items)} || {C, Items} <- Groups])
                    )}
            ]
        end}.

group_batch_without_bucket_tmpl_test_() ->
    BatchData = [{c1, #{payload => #{bucket => <<"b1">>}}}],
    [
        {"a channel config without bucket_tmpl groups everything to the static client",
            ?_assertEqual(
                [{client, BatchData}],
                group_batch_by_bucket(BatchData, #{channel_client => client})
            )}
    ].

data_to_points_test_() ->
    SyntaxLines = [
        #{line => emqx_placeholder:preproc_tmpl(<<"${payload.line}">>)}
    ],
    [
        {"renders a raw line protocol string",
            ?_assertEqual(
                {ok, [
                    <<"weather,location=us-midwest temperature=82 1465839830100400200">>
                ]},
                data_to_points(
                    #{
                        payload => #{
                            line => <<
                                "weather,location=us-midwest "
                                "temperature=82 1465839830100400200"
                            >>
                        }
                    },
                    SyntaxLines
                )
            )},
        {"raw line with trailing newline is trimmed",
            ?_assertEqual(
                {ok, [<<"weather temperature=82 1">>]},
                data_to_points(
                    #{payload => #{line => <<"weather temperature=82 1\n">>}},
                    SyntaxLines
                )
            )},
        {"missing field skips the raw line",
            ?_assertEqual(
                {ok, []},
                data_to_points(#{payload => #{}}, SyntaxLines)
            )},
        {"empty rendered line is skipped",
            ?_assertEqual(
                {ok, []},
                data_to_points(#{payload => #{line => <<>>}}, SyntaxLines)
            )},
        {"non-text rendered values fail the batch instead of silently succeeding",
            ?_assertMatch(
                {error, _},
                data_to_points(#{payload => #{line => true}}, SyntaxLines)
            )},
        {"integer rendered values fail the batch instead of silently succeeding",
            ?_assertMatch(
                {error, _},
                data_to_points(#{payload => #{line => 123}}, SyntaxLines)
            )},
        {"raw lines and structured points are mixed",
            ?_assertEqual(
                {ok, [
                    #{
                        measurement => <<"m">>,
                        tags => #{<<"tag">> => <<"1">>},
                        fields => #{<<"f">> => {int, 2}},
                        timestamp => 1
                    },
                    <<"weather temperature=82 1">>
                ]},
                data_to_points(
                    #{payload => #{line => <<"weather temperature=82 1">>}},
                    [
                        #{line => emqx_placeholder:preproc_tmpl(<<"${payload.line}">>)},
                        #{
                            measurement => emqx_placeholder:preproc_tmpl(<<"m">>),
                            tags => #{
                                emqx_placeholder:preproc_tmpl(<<"tag">>) =>
                                    emqx_placeholder:preproc_tmpl(<<"1">>)
                            },
                            fields => #{
                                emqx_placeholder:preproc_tmpl(<<"f">>) =>
                                    emqx_placeholder:preproc_tmpl(<<"2i">>)
                            },
                            timestamp => emqx_placeholder:preproc_tmpl(<<"1">>),
                            precision => {ms, ms}
                        }
                    ]
                )
            )}
    ].

classify_result_test_() ->
    [
        {"already classified errors are not wrapped again",
            ?_assertEqual(
                {error, {unrecoverable_error, boom}},
                classify_result({error, {unrecoverable_error, boom}})
            )},
        {"already classified recoverable errors are not wrapped again",
            ?_assertEqual(
                {error, {recoverable_error, boom}},
                classify_result({error, {recoverable_error, boom}})
            )}
    ].

grouped_result_aggregation_test_() ->
    Unrecoverable =
        {error, {unrecoverable_error, #{code => 404, body => <<"missing">>}}},
    Recoverable = {error, {recoverable_error, closed}},
    [
        {"a later recoverable error wins over an earlier unrecoverable error",
            ?_assertEqual(
                Recoverable,
                aggregate_group_results([
                    {ok, 404, [], <<"missing">>},
                    {error, closed}
                ])
            )},
        {"an earlier recoverable error wins over a later unrecoverable error",
            ?_assertEqual(
                Recoverable,
                aggregate_group_results([
                    {error, closed},
                    {ok, 404, [], <<"missing">>}
                ])
            )},
        {"only unrecoverable errors are reported as unrecoverable",
            ?_assertEqual(
                Unrecoverable,
                aggregate_group_results([
                    {ok, 204, [], <<>>},
                    {ok, 404, [], <<"missing">>}
                ])
            )},
        {"only successful groups result in ok",
            ?_assertEqual(
                ok,
                aggregate_group_results([
                    {ok, 204, [], <<>>},
                    {ok, 204, [], <<>>}
                ])
            )}
    ].

aggregate_group_results(Results) ->
    grouped_result(lists:foldl(fun record_group_result/2, group_results_acc(), Results)).

grouped_sync_error_priority_test_() ->
    Groups = [
        {#{path => <<"a">>}, [point_a]},
        {#{path => <<"b">>}, [point_b]}
    ],
    with_influxdb_mock(fun(_) ->
        [
            {"sync: a later recoverable error keeps the batch retriable", fun() ->
                ?assertEqual(
                    {error, {recoverable_error, closed}},
                    run_sync_group_results(
                        Groups,
                        [
                            {error, {404, <<"missing">>}},
                            {error, closed}
                        ]
                    )
                )
            end},
            {"sync: an earlier recoverable error keeps the batch retriable", fun() ->
                ?assertEqual(
                    {error, {recoverable_error, closed}},
                    run_sync_group_results(
                        Groups,
                        [
                            {error, closed},
                            {error, {404, <<"missing">>}}
                        ]
                    )
                )
            end}
        ]
    end).

run_sync_group_results(Groups, Results) ->
    TID = ets:new(?MODULE, [set, public]),
    true = ets:insert(TID, {calls, 0}),
    try
        meck:reset(influxdb),
        meck:expect(influxdb, write, fun(_Client, _Points) ->
            Index = ets:update_counter(TID, calls, 1),
            lists:nth(Index, Results)
        end),
        Result = write_groups_sync(inst, chan, Groups),
        ?assertEqual(
            length(Results), meck:num_calls(influxdb, write, ['_', '_'])
        ),
        Result
    after
        ets:delete(TID)
    end.

grouped_async_worker_monitor_test_() ->
    with_influxdb_mock(fun(_) ->
        [
            {"worker DOWN completes the batch with a recoverable error", fun() ->
                Worker = test_worker(),
                ok = meck:expect(influxdb, write_async, fun(_Client, _Points, _Reply) ->
                    {ok, Worker}
                end),
                Self = self(),
                ReplyFunAndArgs = {
                    fun(Result) -> Self ! {grouped_async_reply, Result} end, []
                },
                {ok, Coordinator} = write_groups_async(
                    inst,
                    chan,
                    [{#{path => <<"a">>}, [point]}],
                    ReplyFunAndArgs
                ),
                ?assert(is_pid(Coordinator)),
                exit(Worker, kill),
                Result =
                    receive
                        {grouped_async_reply, AsyncResult} -> AsyncResult
                    after 1000 ->
                        no_reply
                    end,
                ?assertMatch(
                    {error, {recoverable_error, {async_worker_down, _}}},
                    Result
                ),
                ?assertEqual(
                    1, meck:num_calls(influxdb, write_async, ['_', '_', '_'])
                )
            end},
            {"different group workers are all monitored", fun() ->
                meck:reset(influxdb),
                WorkerA = test_worker(),
                WorkerB = test_worker(),
                CallCounter = counters:new(1, [write_concurrency]),
                Self = self(),
                ok = meck:expect(influxdb, write_async, fun(_Client, _Points, Reply) ->
                    multi_worker_write_async(
                        Self, CallCounter, WorkerA, WorkerB, Reply
                    )
                end),
                ReplyFunAndArgs = {
                    fun(Result) -> Self ! {grouped_async_reply, Result} end, []
                },
                {ok, _Coordinator} = write_groups_async(
                    inst,
                    chan,
                    [
                        {#{path => <<"a">>}, [point_a]},
                        {#{path => <<"b">>}, [point_b]}
                    ],
                    ReplyFunAndArgs
                ),
                SecondReply =
                    receive
                        {second_worker_reply, ReplyFunAndArgs1} -> ReplyFunAndArgs1
                    after 1000 ->
                        no_reply
                    end,
                ?assert(is_tuple(SecondReply) andalso tuple_size(SecondReply) =:= 2),
                exit(WorkerA, kill),
                {ReplyFun, ReplyArgs} = SecondReply,
                ok = apply(ReplyFun, ReplyArgs ++ [{ok, 204, [], <<>>}]),
                Result =
                    receive
                        {grouped_async_reply, AsyncResult} -> AsyncResult
                    after 1000 ->
                        no_reply
                    end,
                ?assertMatch(
                    {error, {recoverable_error, {async_worker_down, _}}},
                    Result
                ),
                ?assertEqual(
                    2, meck:num_calls(influxdb, write_async, ['_', '_', '_'])
                )
            end}
        ]
    end).

grouped_async_owner_monitor_test_() ->
    with_influxdb_mock(fun(_) ->
        [
            {"coordinator exits when its owner exits", fun() ->
                Worker = test_worker(),
                ok = meck:expect(influxdb, write_async, fun(_Client, _Points, _Reply) ->
                    {ok, Worker}
                end),
                Self = self(),
                ReplyFunAndArgs = {
                    fun(Result) -> Self ! {unexpected_grouped_reply, Result} end, []
                },
                Owner = spawn(fun() ->
                    {ok, Coordinator} = write_groups_async(
                        inst,
                        chan,
                        [{#{path => <<"a">>}, [point]}],
                        ReplyFunAndArgs
                    ),
                    Self ! {grouped_async_coordinator, Coordinator},
                    wait_for_stop()
                end),
                try
                    Coordinator =
                        receive
                            {grouped_async_coordinator, Pid} -> Pid
                        after 1_000 ->
                            error(grouped_async_coordinator_timeout)
                        end,
                    CoordinatorRef = monitor(process, Coordinator),
                    exit(Owner, kill),
                    receive
                        {'DOWN', CoordinatorRef, process, Coordinator, Reason} ->
                            ?assertEqual(normal, Reason)
                    after 1_000 ->
                        error(grouped_async_coordinator_still_running)
                    end
                after
                    exit(Worker, kill)
                end
            end}
        ]
    end).

wait_for_stop() ->
    receive
        stop -> ok
    after infinity ->
        ok
    end.

test_worker() ->
    spawn(fun() ->
        receive
            stop -> ok
        after infinity ->
            ok
        end
    end).

multi_worker_write_async(Self, CallCounter, WorkerA, WorkerB, Reply) ->
    case counters:get(CallCounter, 1) + 1 of
        1 ->
            counters:add(CallCounter, 1, 1),
            {ok, WorkerA};
        2 ->
            counters:add(CallCounter, 1, 1),
            Self ! {second_worker_reply, Reply},
            {ok, WorkerB}
    end.

empty_points_skip_test_() ->
    with_influxdb_mock(fun(_) ->
        [
            {"an empty point list is not written synchronously", fun() ->
                meck:reset(influxdb),
                ?assertEqual(ok, do_query(inst, chan, client, [])),
                ?assertEqual(0, meck:num_calls(influxdb, write, ['_', '_']))
            end},
            {"an empty point list is not written asynchronously and is acked once", fun() ->
                meck:reset(influxdb),
                ?assertEqual(
                    ok,
                    run_async_reply(fun(ReplyFunAndArgs) ->
                        do_async_query(inst, chan, client, [], ReplyFunAndArgs)
                    end)
                ),
                ?assertEqual(0, meck:num_calls(influxdb, write_async, ['_', '_', '_']))
            end}
        ]
    end).

grouped_preparse_test_() ->
    Syntax = [structured_syntax()],
    OkData = #{payload => #{v => <<"1">>, ts => 1}},
    BadData = #{payload => #{v => <<"1">>, ts => <<"abc">>}},
    ClientA = #{path => <<"a">>},
    ClientB = #{path => <<"b">>},
    ValidGroups = [{ClientA, [{c1, OkData}]}],
    MixedGroups = [{ClientA, [{c1, OkData}]}, {ClientB, [{c2, BadData}]}],
    with_influxdb_mock(fun(_) ->
        [
            {"sync: a later group parse failure prevents every write", fun() ->
                meck:reset(influxdb),
                ?assertMatch(
                    {error, {unrecoverable_error, points_trans_failed}},
                    do_grouped_batch_query(inst, chan, MixedGroups, Syntax)
                ),
                ?assertEqual(0, meck:num_calls(influxdb, write, ['_', '_']))
            end},
            {"async: a later group parse failure replies one error and issues no write", fun() ->
                meck:reset(influxdb),
                ?assertMatch(
                    {error, {unrecoverable_error, points_trans_failed}},
                    run_async_reply(fun(ReplyFunAndArgs) ->
                        do_grouped_async_batch_query(
                            inst, chan, MixedGroups, Syntax, ReplyFunAndArgs
                        )
                    end)
                ),
                ?assertEqual(
                    0, meck:num_calls(influxdb, write_async, ['_', '_', '_'])
                )
            end},
            {"sync: a fully valid batch is still written", fun() ->
                meck:reset(influxdb),
                ?assertEqual(ok, do_grouped_batch_query(inst, chan, ValidGroups, Syntax)),
                ?assertEqual(1, meck:num_calls(influxdb, write, ['_', '_']))
            end}
        ]
    end).

grouped_empty_points_skip_test_() ->
    EmptySyntax = [#{line => emqx_placeholder:preproc_tmpl(<<"${payload.missing}">>)}],
    Groups = [
        {#{path => <<"a">>}, [{c1, #{payload => #{}}}]},
        {#{path => <<"b">>}, [{c2, #{payload => #{}}}]}
    ],
    with_influxdb_mock(fun(_) ->
        [
            {"sync: groups that render no points are a no-op", fun() ->
                meck:reset(influxdb),
                ?assertEqual(ok, do_grouped_batch_query(inst, chan, Groups, EmptySyntax)),
                ?assertEqual(0, meck:num_calls(influxdb, write, ['_', '_']))
            end},
            {"async: groups that render no points reply ok once", fun() ->
                meck:reset(influxdb),
                ?assertEqual(
                    ok,
                    run_async_reply(fun(ReplyFunAndArgs) ->
                        do_grouped_async_batch_query(
                            inst, chan, Groups, EmptySyntax, ReplyFunAndArgs
                        )
                    end)
                ),
                ?assertEqual(0, meck:num_calls(influxdb, write_async, ['_', '_', '_']))
            end}
        ]
    end).

with_influxdb_mock(TestFun) ->
    {setup,
        fun() ->
            ok = meck:new(influxdb, [passthrough, no_link]),
            ok = meck:expect(influxdb, write, fun(_Client, _Points) -> ok end),
            ok = meck:expect(influxdb, write_async, fun(_Client, _Points, _Reply) ->
                {ok, self()}
            end),
            ok
        end,
        fun(_) ->
            meck:unload(influxdb)
        end,
        TestFun}.

structured_syntax() ->
    #{
        measurement => emqx_placeholder:preproc_tmpl(<<"m">>),
        tags => #{},
        fields => #{
            emqx_placeholder:preproc_tmpl(<<"f">>) =>
                emqx_placeholder:preproc_tmpl(<<"${payload.v}">>)
        },
        timestamp => emqx_placeholder:preproc_tmpl(<<"${payload.ts}">>),
        precision => {ms, ms}
    }.

run_async_reply(Run) ->
    Self = self(),
    ReplyFunAndArgs = {fun(Result) -> Self ! {async_reply, Result} end, []},
    ok = Run(ReplyFunAndArgs),
    receive
        {async_reply, Result} -> Result
    after 1000 ->
        no_reply
    end.

%% for coverage
desc_test_() ->
    [
        ?_assertMatch(
            {desc, _, _},
            desc(common)
        ),
        ?_assertMatch(
            {desc, _, _},
            desc(influxdb_api_v1)
        ),
        ?_assertMatch(
            {desc, _, _},
            desc(influxdb_api_v2)
        ),
        ?_assertMatch(
            {desc, _, _},
            hocon_schema:field_schema(server(), desc)
        ),
        ?_assertMatch(
            connector_influxdb,
            namespace()
        )
    ].
-endif.
