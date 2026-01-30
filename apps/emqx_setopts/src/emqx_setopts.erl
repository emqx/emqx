%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_setopts).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0]).
-export([on_message_publish/1]).
-export([set_keepalive/1]).
-export([set_keepalive_batch_async/1]).
-export([do_call_keepalive_clients/1]).
-export([test_extract_batch_client_result/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([keepalive_batch/0]).

-define(SERVER, ?MODULE).
-define(KEEPALIVE_SINGLE, <<"$SETOPTS/mqtt/keepalive">>).
-define(KEEPALIVE_BULK, <<"$SETOPTS/mqtt/keepalive-bulk">>).
-define(MAILBOX_OVERLOAD_LIMIT, 10).
-define(BULK_SYNC_TIMEOUT_MS, 5000).

-type keepalive_interval() :: 0..65535.
-type keepalive_batch_item() :: {emqx_types:clientid(), keepalive_interval()}.
-type keepalive_batch() :: [keepalive_batch_item()].

-doc """
Start the setopts update server.
""".
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc """
Update keepalive interval for a single client.
""".
-spec set_keepalive({emqx_types:clientid(), integer()}) -> ok | {error, term()}.
set_keepalive({ClientId, Interval}) when is_binary(ClientId) ->
    try
        Normalized = normalize_interval(Interval),
        keepalive_sync(ClientId, Normalized)
    catch
        throw:Reason -> {error, Reason}
    end;
set_keepalive(Other) ->
    {error, {invalid_item, Other}}.

-spec set_keepalive_batch_async(keepalive_batch()) -> ok.
set_keepalive_batch_async(Batch) ->
    gen_server:cast(?SERVER, {keepalive_batch_async, Batch}).

do_set_keepalive_batch(Batch) ->
    %% Collect all unique nodes from all clients
    AllNodes = lists:usort(
        lists:foldl(
            fun({ClientId, _Interval}, Acc) ->
                Nodes = lookup_client_nodes(ClientId),
                Acc ++ Nodes
            end,
            [],
            Batch
        )
    ),
    case AllNodes of
        [] ->
            %% No nodes found, return not_found for all clients
            {ok, [{ClientId, {error, not_found}} || {ClientId, _Interval} <- Batch]};
        _ ->
            %% Call bulk API on all nodes
            Results = call_keepalive_clients(AllNodes, Batch),
            %% Extract results for each client
            {ok, [
                {ClientId, extract_batch_client_result(ClientId, Results)}
             || {ClientId, _Interval} <- Batch
            ]}
    end.

do_set_keepalive_single(ClientId, Interval) ->
    case do_set_keepalive_batch([{ClientId, Interval}]) of
        {ok, [{ClientId, Result}]} -> Result;
        {ok, _} -> {error, not_found}
    end.

extract_batch_client_result(ClientId, Results) ->
    %% Results is a list of [{ClientId, Result}] from each node (after unwrap_erpc)
    %% Find the first successful result, or return the first error
    lists:foldl(
        fun
            (NodeResults, Acc) when is_list(NodeResults) ->
                case lists:keyfind(ClientId, 1, NodeResults) of
                    {ClientId, ok} ->
                        ok;
                    {ClientId, {error, _} = Err} ->
                        %% Prefer success if we already have it, otherwise use this error
                        case Acc of
                            ok -> ok;
                            {error, not_found} -> Err;
                            _ -> Acc
                        end;
                    false ->
                        Acc
                end;
            ({error, _} = Err, Acc) ->
                %% Node error, prefer success if we have it
                case Acc of
                    ok -> ok;
                    {error, not_found} -> Err;
                    _ -> Acc
                end;
            (_Other, Acc) ->
                Acc
        end,
        {error, not_found},
        Results
    ).

%% Test helper to exercise batch result folding branches.
test_extract_batch_client_result(ClientId, Results) ->
    extract_batch_client_result(ClientId, Results).

%% Hook callback
-doc """
Handle $SETOPTS system topics and prevent routing to subscribers.
Known keepalive updates are applied; unknown $SETOPTS topics are
ignored with a warning. Non-$SETOPTS publishes pass through.
""".
on_message_publish(#message{topic = <<"$SETOPTS/", _/binary>>} = Msg) ->
    handle_setopts(Msg);
on_message_publish(_Msg) ->
    ok.

%% gen_server callbacks
init([]) ->
    process_flag(trap_exit, true),
    ok = emqx_hooks:put('message.publish', {?MODULE, on_message_publish, []}, ?HP_SYS_MSGS),
    {ok, #{}}.

handle_call(_Req, _From, State) ->
    {reply, ignored, State}.

handle_cast({keepalive_batch_async, Batch}, State) ->
    case process_info(self(), message_queue_len) of
        {message_queue_len, Len} when Len > ?MAILBOX_OVERLOAD_LIMIT ->
            ?SLOG(warning, #{
                msg => "keepalive_update_batch_dropped",
                batch_size => length(Batch),
                queue_len => Len,
                cause => overload
            }),
            {noreply, State};
        _ ->
            _ = do_set_keepalive_batch(Batch),
            {noreply, State}
    end;
handle_cast({keepalive_sync, ClientId, Interval, DeadlineMs, Alias}, State) ->
    case erlang:monotonic_time(millisecond) > DeadlineMs of
        true ->
            {noreply, State};
        false ->
            Reply = do_set_keepalive_single(ClientId, Interval),
            Alias ! {Alias, Reply},
            {noreply, State}
    end;
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish, []}),
    ok.

%% Internal helpers

handle_setopts(#message{topic = ?KEEPALIVE_BULK, payload = Payload} = Msg) ->
    handle_keepalive_bulk(Msg, Payload);
handle_setopts(#message{topic = ?KEEPALIVE_SINGLE, payload = Payload, from = From} = Msg) ->
    handle_keepalive_self(Msg, Payload, From);
handle_setopts(#message{topic = Topic, headers = Headers} = Msg) ->
    ?SLOG(warning, #{msg => "setopts_unknown_topic", topic => Topic}),
    {stop, Msg#message{headers = Headers#{allow_publish => false}}}.

handle_keepalive_self(Msg, Payload, From) ->
    case decode_single_payload(Payload) of
        {ok, Interval} ->
            case From of
                _ClientId when is_binary(From) ->
                    Clamped = clamp_keepalive_for_client(From, Interval),
                    %% Single updates are handled by the publishing process
                    gen_server:cast(self(), {keepalive, Clamped});
                _ ->
                    ?tp(warning, keepalive_update_single_forbidden, #{from => From})
            end,
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}};
        {error, Reason} ->
            ?SLOG(warning, #{msg => "keepalive_update_payload_invalid", reason => Reason}),
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}}
    end.

handle_keepalive_bulk(Msg, Payload) ->
    case decode_bulk_payload(Payload) of
        {ok, Batch} ->
            set_keepalive_batch_async(Batch),
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}};
        {error, Reason} ->
            ?SLOG(warning, #{msg => "keepalive_update_payload_invalid", reason => Reason}),
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}}
    end.

lookup_client_nodes(ClientId) ->
    case emqx_cm_registry:is_enabled() of
        true ->
            Channels = emqx_cm:lookup_channels(ClientId),
            lists:usort([node(Pid) || Pid <- Channels]);
        false ->
            emqx:running_nodes()
    end.

-doc """
RPC entrypoint: apply keepalive updates on a local node.
""".
-spec do_call_keepalive_clients(keepalive_batch()) -> term().
do_call_keepalive_clients(Batch) ->
    [
        {ClientId, do_call_keepalive_local(ClientId, Interval)}
     || {ClientId, Interval} <- Batch
    ].

do_call_keepalive_local(ClientId, Interval) ->
    Channels = emqx_cm:lookup_channels(local, ClientId),
    Connected = lists:filtermap(
        fun(Pid) ->
            case emqx_cm:is_channel_connected(Pid) of
                true ->
                    {true, Pid};
                false ->
                    ?SLOG(debug, #{
                        msg => "keepalive_update_channel_ignored",
                        clientid => ClientId,
                        pid => Pid,
                        reason => not_connected
                    }),
                    false
            end
        end,
        Channels
    ),
    call_connected_channels(ClientId, Connected, {keepalive, Interval}).

call_connected_channels(_ClientId, [], _Req) ->
    {error, not_found};
call_connected_channels(ClientId, Channels, Req) ->
    Results = [cast_channel(ClientId, Pid, Req) || Pid <- Channels],
    {OkAcc, ErrAcc} = lists:foldl(
        fun
            ({error, not_found}, {Ok, Errs}) -> {Ok, Errs};
            ({error, _} = Err, {Ok, Errs}) -> {Ok, [Err | Errs]};
            (OkRes, {Ok, Errs}) -> {[OkRes | Ok], Errs}
        end,
        {[], []},
        Results
    ),
    case OkAcc of
        [Result | _] ->
            Result;
        [] ->
            case ErrAcc of
                [] -> {error, not_found};
                [FirstErr | _] -> FirstErr
            end
    end.

cast_channel(ClientId, Pid, Req) ->
    case emqx_cm:get_chan_info(ClientId, Pid) of
        #{conninfo := #{conn_mod := ConnMod}, clientinfo := #{zone := Zone}} ->
            cast_conn(ConnMod, Pid, clamp_keepalive_for_zone(Req, Zone));
        _ ->
            {error, not_found}
    end.

call_keepalive_clients(Nodes, Batch) ->
    emqx_rpc:unwrap_erpc(emqx_setopts_proto_v1:call_keepalive_clients(Nodes, Batch)).

cast_conn(ConnMod, Pid, {keepalive, _Interval} = Req) ->
    ok = erlang:apply(ConnMod, cast, [Pid, Req]).

keepalive_sync(ClientId, Interval) ->
    DeadlineMs = erlang:monotonic_time(millisecond) + ?BULK_SYNC_TIMEOUT_MS,
    Alias = erlang:alias([reply]),
    gen_server:cast(?SERVER, {keepalive_sync, ClientId, Interval, DeadlineMs, Alias}),
    receive
        {Alias, Reply} ->
            erlang:unalias(Alias),
            Reply
    after ?BULK_SYNC_TIMEOUT_MS ->
        erlang:unalias(Alias),
        {error, timeout}
    end.

clamp_keepalive_for_client(ClientId, Interval) ->
    case emqx_cm:get_chan_info(ClientId) of
        #{clientinfo := #{zone := Zone}} ->
            clamp_keepalive_value(Interval, Zone);
        _ ->
            Interval
    end.

clamp_keepalive_for_zone({keepalive, Interval}, Zone) ->
    {keepalive, clamp_keepalive_value(Interval, Zone)}.

clamp_keepalive_value(Interval, Zone) ->
    case emqx_config:get_zone_conf(Zone, [mqtt, server_keepalive], disabled) of
        Keepalive when is_integer(Keepalive) ->
            min(Interval, Keepalive);
        _ ->
            Interval
    end.

%% Payload decoding

decode_single_payload(Payload) ->
    try
        Interval = normalize_interval(Payload),
        {ok, Interval}
    catch
        throw:Reason -> {error, Reason}
    end.

decode_bulk_payload(Payload) ->
    case emqx_utils_json:safe_decode(Payload, [return_maps]) of
        {ok, List} when is_list(List) ->
            normalize_updates(List);
        {ok, Other} ->
            {error, {invalid_payload, Other}};
        {error, Reason} ->
            {error, Reason}
    end.

normalize_updates(Items) ->
    try
        Updates = [normalize_item(Item) || Item <- Items],
        {ok, Updates}
    catch
        throw:Reason -> {error, Reason}
    end.

normalize_item(#{<<"clientid">> := ClientId, <<"keepalive">> := Interval}) ->
    {ClientId, normalize_interval(Interval)};
normalize_item(#{clientid := ClientId, keepalive := Interval}) ->
    {ClientId, normalize_interval(Interval)};
normalize_item(Other) ->
    erlang:throw({invalid_item, Other}).

normalize_interval(Interval) when is_integer(Interval) ->
    case Interval of
        Value when Value >= 0, Value =< 65535 ->
            Value;
        _ ->
            erlang:throw({invalid_keepalive, Interval})
    end;
normalize_interval(Interval) when is_binary(Interval); is_list(Interval) ->
    Int = to_int(Interval),
    case Int of
        Value when Value >= 0, Value =< 65535 ->
            Value;
        _ ->
            erlang:throw({invalid_keepalive, Interval})
    end;
normalize_interval(Interval) ->
    erlang:throw({invalid_keepalive, Interval}).

to_int(Value) when is_integer(Value) -> Value;
to_int(Value) ->
    try emqx_utils_conv:int(Value) of
        Int -> Int
    catch
        _:_ -> erlang:throw({invalid_keepalive, Value})
    end.
