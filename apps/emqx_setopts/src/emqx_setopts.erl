%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_setopts).

-behaviour(gen_server).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

-export([start_link/0]).
-export([on_message_publish/1]).
-export([set_keepalive/2]).
-export([do_call_keepalive_clients/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([keepalive_batch/0]).

-define(SERVER, ?MODULE).
-define(KEEPALIVE_PREFIX, "$SETOPTS/mqtt/keepalive/").
-define(KEEPALIVE_BULK, <<"$SETOPTS/mqtt/keepalive-bulk">>).
-define(MAILBOX_OVERLOAD_LIMIT, 10).
-define(CALL_CONN_TIMEOUT_MS, 100).

-type keepalive_interval() :: 0..65535.
-type keepalive_batch_item() :: {emqx_types:clientid(), keepalive_interval()}.
-type keepalive_batch() :: [keepalive_batch_item()].

-doc """
Start the setopts update server.
""".
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% Public API for keepalive routing (used by emqx_mgmt if needed)
-doc """
Update a client's keepalive interval (0..65535).
""".
set_keepalive(ClientId, Interval) when is_integer(Interval), Interval >= 0, Interval =< 65535 ->
    do_set_keepalive(ClientId, Interval);
set_keepalive(_ClientId, _Interval) ->
    {error, <<"mqtt3.1.1 specification: keepalive must between 0~65535">>}.

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

handle_cast({update_keepalive, Updates}, State) ->
    case process_info(self(), message_queue_len) of
        {message_queue_len, Len} when Len > ?MAILBOX_OVERLOAD_LIMIT ->
            ?SLOG(warning, #{
                msg => "keepalive_update_batch_dropped",
                batch_size => length(Updates),
                queue_len => Len,
                cause => overload
            }),
            {noreply, State};
        _ ->
            lists:foreach(fun handle_update/1, Updates),
            {noreply, State}
    end.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish, []}),
    ok.

handle_update(#{clientid := ClientId, keepalive := Interval}) ->
    case set_keepalive(ClientId, Interval) of
        ok ->
            ok;
        {error, not_found} ->
            ?SLOG(debug, #{msg => "keepalive_update_client_not_found", clientid => ClientId});
        {error, Reason} ->
            ?SLOG(debug, #{msg => "keepalive_update_failed", clientid => ClientId, reason => Reason})
    end;
handle_update(Invalid) ->
    ?SLOG(debug, #{msg => "keepalive_update_item_invalid", item => Invalid}),
    ok.

%% Internal helpers

handle_setopts(#message{topic = ?KEEPALIVE_BULK, payload = Payload} = Msg) ->
    handle_keepalive_bulk(Msg, Payload);
handle_setopts(
    #message{topic = <<?KEEPALIVE_PREFIX, ClientId/binary>>, payload = Payload} = Msg
) when
    ClientId =/= <<>>
->
    handle_keepalive_single(Msg, ClientId, Payload);
handle_setopts(#message{topic = Topic, headers = Headers} = Msg) ->
    ?SLOG(warning, #{msg => "setopts_unknown_topic", topic => Topic}),
    {stop, Msg#message{headers = Headers#{allow_publish => false}}}.

handle_keepalive_single(Msg, ClientId, Payload) ->
    case decode_single_payload(Payload, ClientId) of
        {ok, Updates} ->
            gen_server:cast(?SERVER, {update_keepalive, Updates}),
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}};
        {error, Reason} ->
            ?SLOG(warning, #{msg => "keepalive_update_payload_invalid", reason => Reason}),
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}}
    end.

handle_keepalive_bulk(Msg, Payload) ->
    case decode_bulk_payload(Payload) of
        {ok, Updates} ->
            gen_server:cast(?SERVER, {update_keepalive, Updates}),
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}};
        {error, Reason} ->
            ?SLOG(warning, #{msg => "keepalive_update_payload_invalid", reason => Reason}),
            Headers = Msg#message.headers,
            {stop, Msg#message{headers = Headers#{allow_publish => false}}}
    end.

do_set_keepalive(ClientId, Interval) ->
    Nodes = lookup_client_nodes(ClientId),
    Batch = [{ClientId, Interval}],
    call_keepalive_on_nodes(Nodes, ClientId, Batch).

lookup_client_nodes(ClientId) ->
    case emqx_cm_registry:is_enabled() of
        true ->
            Channels = emqx_cm:lookup_channels(ClientId),
            lists:usort([node(Pid) || Pid <- Channels]);
        false ->
            emqx:running_nodes()
    end.

call_keepalive_on_nodes([], _ClientId, _Batch) ->
    {error, not_found};
call_keepalive_on_nodes(Nodes, ClientId, Batch) ->
    Results = call_keepalive_clients(Nodes, Batch),
    {Expected, Errs} = lists:foldr(
        fun({N, Res}, Acc) ->
            case extract_client_result(ClientId, Res) of
                {error, not_found} ->
                    Acc;
                {error, _} = Err ->
                    {OkAcc, ErrAcc} = Acc,
                    {OkAcc, [{N, Err} | ErrAcc]};
                OkRes ->
                    {OkAcc, ErrAcc} = Acc,
                    {[OkRes | OkAcc], ErrAcc}
            end
        end,
        {[], []},
        lists:zip(Nodes, Results)
    ),
    case Expected of
        [] ->
            case Errs of
                [] -> {error, not_found};
                [{_Node, FirstErr} | _] -> FirstErr
            end;
        [Result | _] ->
            Result
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
    Results = [call_channel(ClientId, Pid, Req) || Pid <- Channels],
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

call_channel(ClientId, Pid, Req) ->
    case emqx_cm:get_chan_info(ClientId, Pid) of
        #{conninfo := #{conn_mod := ConnMod}} ->
            call_conn(ConnMod, Pid, Req);
        _ ->
            {error, not_found}
    end.

call_keepalive_clients(Nodes, Batch) ->
    emqx_rpc:unwrap_erpc(emqx_setopts_proto_v1:call_keepalive_clients(Nodes, Batch)).

extract_client_result(_ClientId, {error, _} = Err) ->
    Err;
extract_client_result(ClientId, Res) when is_list(Res) ->
    case lists:keyfind(ClientId, 1, Res) of
        {ClientId, ClientRes} ->
            ClientRes;
        false ->
            {error, not_found}
    end.

call_conn(ConnMod, Pid, Req) ->
    try
        %% Keep calls short to avoid slowing batch updates if a client is stuck.
        erlang:apply(ConnMod, call, [Pid, Req, ?CALL_CONN_TIMEOUT_MS])
    catch
        exit:R when R =:= shutdown; R =:= normal ->
            {error, shutdown};
        exit:{R, _} when R =:= shutdown; R =:= noproc ->
            {error, shutdown};
        exit:{{shutdown, _OOMInfo}, _Location} ->
            {error, shutdown};
        exit:timeout ->
            LogData = #{
                msg => "call_client_connection_process_timeout",
                request => Req,
                pid => Pid,
                module => ConnMod
            },
            LogData1 =
                case node(Pid) =:= node() of
                    true ->
                        LogData#{stacktrace => erlang:process_info(Pid, current_stacktrace)};
                    false ->
                        LogData
                end,
            ?SLOG(warning, LogData1),
            {error, timeout};
        exit:{timeout, _} ->
            LogData = #{
                msg => "call_client_connection_process_timeout",
                request => Req,
                pid => Pid,
                module => ConnMod
            },
            LogData1 =
                case node(Pid) =:= node() of
                    true ->
                        LogData#{stacktrace => erlang:process_info(Pid, current_stacktrace)};
                    false ->
                        LogData
                end,
            ?SLOG(warning, LogData1),
            {error, timeout}
    end.

%% Payload decoding

decode_single_payload(Payload, ClientId) ->
    try
        Interval = to_int(Payload),
        {ok, [#{clientid => ClientId, keepalive => Interval}]}
    catch
        error:Reason -> {error, Reason}
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
        error:Reason -> {error, Reason}
    end.

normalize_item(#{<<"clientid">> := ClientId, <<"keepalive">> := Interval}) ->
    #{clientid => ClientId, keepalive => to_int(Interval)};
normalize_item(#{clientid := ClientId, keepalive := Interval}) ->
    #{clientid => ClientId, keepalive => to_int(Interval)};
normalize_item(Other) ->
    erlang:error({invalid_item, Other}).

to_int(Value) when is_integer(Value) -> Value;
to_int(Value) ->
    try emqx_utils_conv:int(Value) of
        Int -> Int
    catch
        _:_ -> erlang:error({invalid_keepalive, Value})
    end.
