%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_connector_hstreamdb).

-include_lib("hocon/include/hoconsc.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-import(hoconsc, [mk/2, enum/1]).

-behaviour(emqx_resource).

%% callbacks of behaviour emqx_resource
-export([
    callback_mode/0,
    on_start/2,
    on_stop/2,
    on_query/3,
    on_get_status/2
]).

-export([
    on_flush_result/1
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1,
    connector_examples/1
]).

%% -------------------------------------------------------------------------------------------------
%% resource callback
callback_mode() -> always_sync.

on_start(InstId, Config) ->
    start_client(InstId, Config).

on_stop(InstId, #{client := Client, producer := Producer}) ->
    StopClientRes = hstreamdb:stop_client(Client),
    StopProducerRes = hstreamdb:stop_producer(Producer),
    ?SLOG(info, #{
        msg => "stop hstreamdb connector",
        connector => InstId,
        client => Client,
        producer => Producer,
        stop_client => StopClientRes,
        stop_producer => StopProducerRes
    }).

on_query(
    _InstId,
    {send_message, Data},
    #{producer := Producer, ordering_key := OrderingKey, payload := Payload}
) ->
    Record = to_record(OrderingKey, Payload, Data),
    do_append(Producer, Record).

on_get_status(_InstId, #{client := Client}) ->
    case is_alive(Client) of
        true ->
            connected;
        false ->
            disconnected
    end.

%% -------------------------------------------------------------------------------------------------
%% hstreamdb batch callback
%% TODO: maybe remove it after disk cache is ready

on_flush_result({{flush, _Stream, _Records}, {ok, _Resp}}) ->
    ok;
on_flush_result({{flush, _Stream, _Records}, {error, _Reason}}) ->
    ok.

%% -------------------------------------------------------------------------------------------------
%% schema
namespace() -> connector_hstreamdb.

roots() ->
    fields(config).

fields(config) ->
    [
        {url, mk(binary(), #{required => true, desc => ?DESC("url")})},
        {stream, mk(binary(), #{required => true, desc => ?DESC("stream_name")})},
        {ordering_key, mk(binary(), #{required => false, desc => ?DESC("ordering_key")})},
        {pool_size, mk(pos_integer(), #{required => true, desc => ?DESC("pool_size")})}
    ];
fields("get") ->
    fields("post");
fields("put") ->
    fields(config);
fields("post") ->
    [
        {type, mk(hstreamdb, #{required => true, desc => ?DESC("type")})},
        {name, mk(binary(), #{required => true, desc => ?DESC("name")})}
    ] ++ fields("put").

connector_examples(Method) ->
    [
        #{
            <<"hstreamdb">> => #{
                summary => <<"HStreamDB Connector">>,
                value => values(Method)
            }
        }
    ].

values(post) ->
    maps:merge(values(put), #{name => <<"connector">>});
values(get) ->
    values(post);
values(put) ->
    #{
        type => hstreamdb,
        url => <<"http://127.0.0.1:6570">>,
        stream => <<"stream1">>,
        ordering_key => <<"some_key">>,
        pool_size => 8
    };
values(_) ->
    #{}.

desc(config) ->
    ?DESC("config").

%% -------------------------------------------------------------------------------------------------
%% internal functions
start_client(InstId, Config) ->
    try
        do_start_client(InstId, Config)
    catch
        E:R:S ->
            Error = #{
                msg => "start hstreamdb connector error",
                connector => InstId,
                error => E,
                reason => R,
                stack => S
            },
            ?SLOG(error, Error),
            {error, Error}
    end.

do_start_client(InstId, Config = #{url := Server, pool_size := PoolSize}) ->
    ?SLOG(info, #{
        msg => "starting hstreamdb connector: client",
        connector => InstId,
        config => Config
    }),
    ClientName = client_name(InstId),
    ClientOptions = [
        {url, binary_to_list(Server)},
        {rpc_options, #{pool_size => PoolSize}}
    ],
    case hstreamdb:start_client(ClientName, ClientOptions) of
        {ok, Client} ->
            case is_alive(Client) of
                true ->
                    ?SLOG(info, #{
                        msg => "hstreamdb connector: client started",
                        connector => InstId,
                        client => Client
                    }),
                    start_producer(InstId, Client, Config);
                _ ->
                    ?SLOG(error, #{
                        msg => "hstreamdb connector: client not alive",
                        connector => InstId
                    }),
                    {error, connect_failed}
            end;
        {error, {already_started, Pid}} ->
            ?SLOG(info, #{
                msg => "starting hstreamdb connector: client, find old client. restart client",
                old_client_pid => Pid,
                old_client_name => ClientName
            }),
            _ = hstreamdb:stop_client(ClientName),
            start_client(InstId, Config);
        {error, Error} ->
            ?SLOG(error, #{
                msg => "hstreamdb connector: client failed",
                connector => InstId,
                reason => Error
            }),
            {error, Error}
    end.

is_alive(Client) ->
    case hstreamdb:echo(Client) of
        {ok, _Echo} ->
            true;
        _ErrorEcho ->
            false
    end.

start_producer(
    InstId,
    Client,
    Options = #{stream := Stream, pool_size := PoolSize, egress := #{payload := PayloadBin}}
) ->
    %% TODO: change these batch options after we have better disk cache.
    BatchSize = maps:get(batch_size, Options, 100),
    Interval = maps:get(batch_interval, Options, 1000),
    ProducerOptions = [
        {stream, Stream},
        {callback, {?MODULE, on_flush_result, []}},
        {max_records, BatchSize},
        {interval, Interval},
        {pool_size, PoolSize}
    ],
    Name = produce_name(InstId),
    ?SLOG(info, #{
        msg => "starting hstreamdb connector: producer",
        connector => InstId
    }),
    case hstreamdb:start_producer(Client, Name, ProducerOptions) of
        {ok, Producer} ->
            ?SLOG(info, #{
                msg => "hstreamdb connector: producer started"
            }),
            EnableBatch = maps:get(enable_batch, Options, false),
            Payload = emqx_plugin_libs_rule:preproc_tmpl(PayloadBin),
            OrderingKeyBin = maps:get(ordering_key, Options, <<"">>),
            OrderingKey = emqx_plugin_libs_rule:preproc_tmpl(OrderingKeyBin),
            State = #{
                client => Client,
                producer => Producer,
                enable_batch => EnableBatch,
                ordering_key => OrderingKey,
                payload => Payload
            },
            {ok, State};
        {error, {already_started, Pid}} ->
            ?SLOG(info, #{
                msg =>
                    "starting hstreamdb connector: producer, find old producer. restart producer",
                old_producer_pid => Pid,
                old_producer_name => Name
            }),
            _ = hstreamdb:stop_producer(Name),
            start_producer(InstId, Client, Options);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "starting hstreamdb connector: producer, failed",
                reason => Reason
            }),
            {error, Reason}
    end.

to_record(OrderingKeyTmpl, PayloadTmpl, Data) ->
    OrderingKey = emqx_plugin_libs_rule:proc_tmpl(OrderingKeyTmpl, Data),
    Payload = emqx_plugin_libs_rule:proc_tmpl(PayloadTmpl, Data),
    to_record(OrderingKey, Payload).

to_record(OrderingKey, Payload) when is_binary(OrderingKey) ->
    to_record(binary_to_list(OrderingKey), Payload);
to_record(OrderingKey, Payload) ->
    hstreamdb:to_record(OrderingKey, raw, Payload).

do_append(Producer, Record) ->
    do_append(false, Producer, Record).

%% TODO: this append is async, remove or change it after we have better disk cache.
% do_append(true, Producer, Record) ->
%     case hstreamdb:append(Producer, Record) of
%         ok ->
%             ?SLOG(debug, #{
%                 msg => "hstreamdb producer async append success",
%                 record => Record
%             });
%         {error, Reason} = Err ->
%             ?SLOG(error, #{
%                 msg => "hstreamdb producer async append failed",
%                 reason => Reason,
%                 record => Record
%             }),
%             Err
%     end;
do_append(false, Producer, Record) ->
    %% TODO: this append is sync, but it does not support [Record], can only append one Record.
    %% Change it after we have better dick cache.
    case hstreamdb:append_flush(Producer, Record) of
        {ok, _} ->
            ?SLOG(debug, #{
                msg => "hstreamdb producer sync append success",
                record => Record
            });
        {error, Reason} = Err ->
            ?SLOG(error, #{
                msg => "hstreamdb producer sync append failed",
                reason => Reason,
                record => Record
            }),
            Err
    end.

client_name(InstId) ->
    "client:" ++ to_string(InstId).

produce_name(ActionId) ->
    list_to_atom("producer:" ++ to_string(ActionId)).

to_string(List) when is_list(List) -> List;
to_string(Bin) when is_binary(Bin) -> binary_to_list(Bin);
to_string(Atom) when is_atom(Atom) -> atom_to_list(Atom).
