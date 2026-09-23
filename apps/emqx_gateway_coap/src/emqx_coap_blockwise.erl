%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_coap_blockwise).

-export([
    new/1,
    default_opts/1,
    server_in/3,
    server_followup_in/3,
    server_incoming/3,
    server_prepare_out_response/4,
    client_prepare_out_request/3,
    client_in_response/3,
    expire/2,
    has_active_client_tx/2,
    block1_required/2,
    blockwise_size/1,
    max_body_size/1,
    enabled/1,
    merge/2,
    merge_within_limits/2,
    next_expiry/1
]).

-export_type([state/0]).

-include("emqx_coap.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DEFAULT_MAX_BLOCK_SIZE, 1024).
-define(DEFAULT_MAX_BODY_SIZE, 4 * 1024 * 1024).
-define(DEFAULT_MAX_CONCURRENT_EXCHANGES, 16).
-define(DEFAULT_MAX_TOTAL_SIZE, 16 * 1024 * 1024).
-define(DEFAULT_EXCHANGE_LIFETIME, 247000).

-type block_key() :: term().

-type state() ::
    #{}
    | #{
        opts := map(),
        server_rx_block1 := #{block_key() => map()},
        server_tx_block2 := #{block_key() => map()},
        client_tx_block1 := #{block_key() => map()},
        client_rx_block2 := #{block_key() => map()},
        client_req := #{block_key() => map()}
    }.

-spec new(map()) -> state().
new(Opts0) ->
    Opts = normalize_opts(Opts0),
    #{
        opts => Opts,
        server_rx_block1 => #{},
        server_tx_block2 => #{},
        client_tx_block1 => #{},
        client_rx_block2 => #{},
        client_req => #{}
    }.

-spec default_opts(coap | lwm2m) -> map().
default_opts(coap) ->
    BlockwiseCfg = emqx:get_config([gateway, coap, blockwise], #{}),
    maps:merge(base_default_opts(), BlockwiseCfg);
default_opts(lwm2m) ->
    BlockwiseCfg = emqx:get_config([gateway, lwm2m, blockwise], #{}),
    LegacyMaxSize = emqx:get_config([gateway, lwm2m, coap_max_block_size], ?DEFAULT_MAX_BLOCK_SIZE),
    maps:merge((base_default_opts())#{max_block_size => LegacyMaxSize}, BlockwiseCfg).

base_default_opts() ->
    #{
        enable => true,
        max_block_size => ?DEFAULT_MAX_BLOCK_SIZE,
        max_body_size => ?DEFAULT_MAX_BODY_SIZE,
        max_concurrent_exchanges => ?DEFAULT_MAX_CONCURRENT_EXCHANGES,
        max_total_size => ?DEFAULT_MAX_TOTAL_SIZE,
        exchange_lifetime => ?DEFAULT_EXCHANGE_LIFETIME
    }.

-spec enabled(state()) -> boolean().
enabled(#{opts := Opts}) -> maps:get(enable, Opts, true).

-spec blockwise_size(state()) -> pos_integer().
blockwise_size(#{opts := Opts}) -> maps:get(max_block_size, Opts, ?DEFAULT_MAX_BLOCK_SIZE).

-spec merge(state(), state()) -> state().
merge(Left, Right) ->
    MapNames = [
        server_rx_block1,
        server_tx_block2,
        client_tx_block1,
        client_rx_block2,
        client_req
    ],
    lists:foldl(
        fun(Name, Acc) ->
            Acc#{Name => maps:merge(maps:get(Name, Left, #{}), maps:get(Name, Right, #{}))}
        end,
        Left,
        MapNames
    ).

-spec merge_within_limits(state(), state()) ->
    {ok, state()} | {error, too_many_exchanges | total_size_exhausted}.
merge_within_limits(Left, Right) ->
    Merged = merge(Left, Right),
    case admission(Merged) of
        ok -> {ok, Merged};
        {error, Reason} -> {error, Reason}
    end.

next_expiry(State) ->
    Maps = [server_rx_block1, server_tx_block2, client_tx_block1, client_rx_block2, client_req],
    Expiries = [
        maps:get(expires_at, Item)
     || Name <- Maps, Item <- maps:values(maps:get(Name, State))
    ],
    case Expiries of
        [] -> undefined;
        _ -> lists:min(Expiries)
    end.

-spec max_body_size(state()) -> pos_integer().
max_body_size(#{opts := Opts}) -> maps:get(max_body_size, Opts, ?DEFAULT_MAX_BODY_SIZE).

-spec has_active_client_tx(term(), state()) -> boolean().
has_active_client_tx(Ctx, #{client_tx_block1 := TxMap}) ->
    maps:is_key(client_tx_key(Ctx), TxMap).

-spec block1_required(coap_message(), state()) -> boolean().
block1_required(#coap_message{payload = Payload, options = Opts}, State) ->
    enabled(State) andalso
        byte_size(Payload) > blockwise_size(State) andalso
        not maps:is_key(block1, Opts).

-spec server_in(coap_message(), term(), state()) ->
    {pass, coap_message(), state()}
    | {continue, coap_message(), state()}
    | {complete, coap_message(), state()}
    | {error, coap_message(), state()}.
server_in(Msg, PeerKey, State0) ->
    State = maybe_expire(State0),
    case enabled(State) of
        false ->
            {pass, Msg, State};
        true ->
            case emqx_coap_message:get_option(block1, Msg, undefined) of
                undefined ->
                    {pass, Msg, State};
                Block ->
                    handle_server_block1(Block, Msg, PeerKey, State)
            end
    end.

-spec server_followup_in(coap_message(), term(), state()) ->
    {pass, coap_message(), state()}
    | {reply, coap_message(), state()}
    | {error, coap_message(), state()}.
server_followup_in(Msg, _PeerKey, State0) when not is_record(Msg, coap_message) ->
    {pass, Msg, maybe_expire(State0)};
server_followup_in(Msg, PeerKey, State0) ->
    State = maybe_expire(State0),
    case enabled(State) of
        false ->
            {pass, Msg, State};
        true ->
            case emqx_coap_message:get_option(block2, Msg, undefined) of
                undefined ->
                    {pass, Msg, State};
                Block2 ->
                    handle_server_followup_block2(Block2, Msg, PeerKey, State)
            end
    end.

-spec server_incoming(coap_message(), term(), state()) ->
    {pass, coap_message(), state()}
    | {complete, coap_message(), state()}
    | {continue, coap_message(), state()}
    | {reply, coap_message(), state()}
    | {error, coap_message(), state()}.
server_incoming(Msg, _PeerKey, State0) when not is_record(Msg, coap_message) ->
    {pass, Msg, maybe_expire(State0)};
server_incoming(Msg, PeerKey, State0) ->
    case server_followup_in(Msg, PeerKey, State0) of
        {pass, Msg1, State1} ->
            server_in(Msg1, PeerKey, State1);
        Other ->
            Other
    end.

-spec server_prepare_out_response(coap_message() | undefined, coap_message(), term(), state()) ->
    {single, coap_message(), state()}
    | {chunked, coap_message(), state()}
    | {error, coap_message(), state()}
    | {busy, atom(), state()}
    | {too_large, state()}.
server_prepare_out_response(_Req, Reply, _PeerKey, State0) when
    not is_record(Reply, coap_message)
->
    State = maybe_expire(State0),
    {single, Reply, State};
server_prepare_out_response(Req, Reply, PeerKey, State0) ->
    State = maybe_expire(State0),
    case enabled(State) andalso should_split_server_tx_block2(Req, Reply, State) of
        false ->
            State1 = maybe_clear_server_tx(PeerKey, Req, State),
            {single, Reply, State1};
        true ->
            handle_server_tx_block2(Req, Reply, PeerKey, State)
    end.

-spec client_prepare_out_request(term(), coap_message(), state()) ->
    {single, coap_message(), state()}
    | {first_block, coap_message(), state()}
    | {busy, too_many_exchanges | total_size_exhausted, state()}
    | {error, too_large, state()}.
client_prepare_out_request(Ctx, Msg, State0) ->
    State = maybe_expire(State0),
    case
        {block1_required(Msg, State), byte_size(Msg#coap_message.payload) > max_total_size(State)}
    of
        {true, true} ->
            {error, too_large, State};
        _ ->
            State1 = put_client_request(Ctx, Msg, State),
            case block1_required(Msg, State) of
                false ->
                    {single, Msg, State1};
                true ->
                    Size = blockwise_size(State),
                    Payload = Msg#coap_message.payload,
                    Key = client_tx_key(Ctx),
                    First = emqx_coap_message:set_payload_block(
                        Payload, block1, {0, true, Size}, Msg
                    ),
                    Tx = #{
                        payload => Payload,
                        size => Size,
                        next_num => 1,
                        req => Msg,
                        expires_at => expires_at(State)
                    },
                    case try_put(client_tx_block1, Key, Tx, State1) of
                        {ok, State2} -> {first_block, First, State2};
                        {error, Reason} -> {busy, Reason, State}
                    end
            end
    end.

-spec client_in_response(term(), coap_message(), state()) ->
    {deliver, coap_message(), state()}
    | {send_next, coap_message(), state()}.
client_in_response(Ctx, Resp, State0) ->
    State = maybe_expire(State0),
    case enabled(State) of
        false ->
            {deliver, Resp, clear_client_exchange(Ctx, State)};
        true ->
            Key = client_tx_key(Ctx),
            case maps:get(Key, maps:get(client_tx_block1, State), undefined) of
                undefined ->
                    maybe_handle_rx_block2(Ctx, Resp, State);
                Tx ->
                    handle_client_tx_block1(Ctx, Resp, Key, Tx, State)
            end
    end.

-spec expire(integer(), state()) -> state().
expire(Now, State) ->
    ServerMap = filter_expired(maps:get(server_rx_block1, State), Now),
    ServerTx = filter_expired(maps:get(server_tx_block2, State), Now),
    ClientTx = filter_expired(maps:get(client_tx_block1, State), Now),
    ClientRx = filter_expired(maps:get(client_rx_block2, State), Now),
    ClientReq = filter_expired(maps:get(client_req, State), Now),
    State#{
        server_rx_block1 => ServerMap,
        server_tx_block2 => ServerTx,
        client_tx_block1 => ClientTx,
        client_rx_block2 => ClientRx,
        client_req => ClientReq
    }.

filter_expired(Map, Now) ->
    maps:filter(fun(_, Tx) -> maps:get(expires_at, Tx, 0) > Now end, Map).

should_split_server_tx_block2(_Req, #coap_message{method = Method}, _State) when
    not is_tuple(Method)
->
    false;
should_split_server_tx_block2(Req, #coap_message{payload = Payload}, State) when
    is_binary(Payload)
->
    case pick_server_tx_block2_params(Req, State) of
        {ok, Num, Size} ->
            byte_size(Payload) > Size orelse Num > 0 orelse has_block2_option(Req);
        error ->
            false
    end;
should_split_server_tx_block2(_Req, _Reply, _State) ->
    false.

handle_server_tx_block2(Req, Reply, PeerKey, State) ->
    {ok, Num, Size} = pick_server_tx_block2_params(Req, State),
    case server_tx_key(PeerKey, Req, Reply) of
        {error, no_token} ->
            case request_block2(Req) of
                undefined ->
                    {single, Reply, State};
                _ ->
                    do_handle_server_tx_block2_stateless(Req, Reply, Num, Size, State)
            end;
        Key ->
            do_handle_server_tx_block2(Key, Req, Reply, Num, Size, PeerKey, State)
    end.

do_handle_server_tx_block2(Key, Req, Reply, Num, Size, _PeerKey, State) ->
    Tx = new_server_tx(Reply, Size, State),
    case build_server_tx_block(Req, Reply, Num, Tx) of
        {ok, ChunkedReply, More} ->
            case try_put_server_tx(Key, Tx, More, State) of
                {ok, State1} ->
                    ?SLOG(debug, #{
                        msg => "coap_block2_tx_chunked",
                        key => Key,
                        block_num => Num,
                        block_size => Size,
                        more => More
                    }),
                    {chunked, ChunkedReply, State1};
                {error, Reason} ->
                    case byte_size(Reply#coap_message.payload) > max_total_size(State) of
                        true -> {too_large, State};
                        false -> {busy, Reason, State}
                    end
            end;
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "coap_block2_tx_build_failed",
                reason => Reason,
                block_num => Num,
                block_size => Size
            }),
            {error, error_reply({error, bad_option}, reply_request(Req, Reply)), State}
    end.

do_handle_server_tx_block2_stateless(Req, Reply, Num, Size, State) ->
    Tx = new_server_tx(Reply, Size, State),
    case build_server_tx_block(Req, Reply, Num, Tx) of
        {ok, ChunkedReply, _More} ->
            ?SLOG(debug, #{
                msg => "coap_block2_tx_chunked_stateless",
                block_num => Num,
                block_size => Size
            }),
            {chunked, ChunkedReply, State};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "coap_block2_tx_build_failed_no_token",
                reason => Reason,
                block_num => Num,
                block_size => Size
            }),
            {error, error_reply({error, bad_option}, reply_request(Req, Reply)), State}
    end.

new_server_tx(Reply, Size, State) ->
    Opts = maps:remove(block2, Reply#coap_message.options),
    #{
        payload => Reply#coap_message.payload,
        size => Size,
        method => Reply#coap_message.method,
        options => Opts,
        observe => maps:get(observe, Opts, undefined),
        expires_at => expires_at(State)
    }.

handle_server_followup_block2({Num, More, Size}, Msg, PeerKey, State) when
    is_integer(Num), Num >= 0, is_boolean(More), is_integer(Size)
->
    case is_valid_block_size(Size) of
        false ->
            {error, error_reply({error, bad_option}, Msg), State};
        true ->
            do_handle_server_followup_block2(Num, Size, Msg, PeerKey, State)
    end;
handle_server_followup_block2(_, Msg, _PeerKey, State) ->
    {error, error_reply({error, bad_option}, Msg), State}.

do_handle_server_followup_block2(Num, Size, Msg, PeerKey, State) ->
    case server_tx_key_from_req(PeerKey, Msg) of
        {error, no_token} ->
            {pass, Msg, State};
        Key ->
            serve_server_followup_block2(Key, Num, Size, Msg, State)
    end.

serve_server_followup_block2(Key, Num, Size, Msg, State) ->
    TxMap = maps:get(server_tx_block2, State),
    case maps:get(Key, TxMap, undefined) of
        undefined ->
            {pass, Msg, State};
        #{size := TxSize} = Tx ->
            EffectiveSize = min(Size, min(TxSize, blockwise_size(State))),
            EffectiveNum = Num * Size div EffectiveSize,
            Tx1 = Tx#{size => EffectiveSize},
            case build_server_tx_block(Msg, undefined, EffectiveNum, Tx1) of
                {ok, Reply, More} ->
                    Tx2 = Tx1#{expires_at => expires_at(State)},
                    case try_put_server_tx(Key, Tx2, More, State) of
                        {ok, State1} ->
                            ?SLOG(debug, #{
                                msg => "coap_block2_followup_served",
                                key => Key,
                                block_num => Num,
                                block_size => Size,
                                more => More
                            }),
                            {reply, Reply, State1};
                        {error, _Reason} ->
                            {error, error_reply({error, service_unavailable}, Msg), State}
                    end;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "coap_block2_followup_failed",
                        key => Key,
                        reason => Reason,
                        block_num => Num,
                        block_size => Size
                    }),
                    State1 = State#{server_tx_block2 => maps:remove(Key, TxMap)},
                    {error, error_reply({error, bad_option}, Msg), State1}
            end
    end.

build_server_tx_block(
    #coap_message{} = Req,
    _Reply,
    Num,
    Tx = #{method := Method, options := Opts}
) ->
    Template0 = emqx_coap_message:piggyback(Method, Req),
    Template = Template0#coap_message{options = maps:remove(block2, Opts)},
    build_server_tx_block_from_template(Template, Num, Tx);
build_server_tx_block(_Req, #coap_message{options = Opts} = Reply, Num, Tx) ->
    Reply0 = Reply#coap_message{options = maps:remove(block2, Opts)},
    build_server_tx_block_from_template(Reply0, Num, Tx).

build_server_tx_block_from_template(Template, Num, #{payload := Payload, size := Size}) ->
    Offset = Num * Size,
    case Offset >= byte_size(Payload) of
        true ->
            {error, out_of_range};
        false ->
            Resp0 = apply_server_tx_template(Template, Payload, Num, Size),
            {_, More, _} = emqx_coap_message:get_option(block2, Resp0, undefined),
            {ok, Resp0, More}
    end.

apply_server_tx_template(#coap_message{} = Reply, Payload, Num, Size) ->
    emqx_coap_message:set_payload_block(Payload, block2, {Num, false, Size}, Reply).

pick_server_tx_block2_params(Req, State) ->
    DefaultSize = blockwise_size(State),
    case request_block2(Req) of
        undefined ->
            {ok, 0, DefaultSize};
        {Num, _More, Size} when
            is_integer(Num), Num >= 0, is_boolean(_More), is_integer(Size)
        ->
            case is_valid_block_size(Size) of
                true ->
                    EffectiveSize = min(Size, DefaultSize),
                    {ok, Num * Size div EffectiveSize, EffectiveSize};
                false ->
                    error
            end;
        _ ->
            error
    end.

request_block2(#coap_message{} = Req) -> emqx_coap_message:get_option(block2, Req, undefined);
request_block2(_) -> undefined.

has_block2_option(#coap_message{} = Req) ->
    emqx_coap_message:get_option(block2, Req, undefined) =/= undefined;
has_block2_option(_) ->
    false.

reply_request(#coap_message{} = Req, _Reply) -> Req;
reply_request(_Req, #coap_message{} = Reply) -> Reply.

maybe_clear_server_tx(PeerKey, Req, State) ->
    case server_tx_key_from_req(PeerKey, Req) of
        {error, no_token} ->
            State;
        Key ->
            TxMap = maps:get(server_tx_block2, State),
            State#{server_tx_block2 => maps:remove(Key, TxMap)}
    end.

try_put_server_tx(Key, _Tx, false, State) ->
    TxMap = maps:get(server_tx_block2, State),
    {ok, State#{server_tx_block2 => maps:remove(Key, TxMap)}};
try_put_server_tx(Key, Tx, true, State) ->
    TxMap = maps:get(server_tx_block2, State),
    Observe = maps:get(observe, Tx, undefined),
    case maps:get(Key, TxMap, undefined) of
        #{observe := Observe} = Prev when Observe =/= undefined ->
            {ok, State#{
                server_tx_block2 => TxMap#{
                    Key => Prev#{expires_at => maps:get(expires_at, Tx)}
                }
            }};
        _ ->
            try_put(server_tx_block2, Key, Tx, State)
    end.

handle_server_block1({Num, More, Size}, Msg, PeerKey, State) when
    is_integer(Num), Num >= 0, is_boolean(More), is_integer(Size)
->
    case is_valid_block_size(Size) of
        false ->
            {error, error_reply({error, bad_option}, Msg), State};
        true ->
            case Size > blockwise_size(State) of
                true ->
                    Reply = too_large_block1_reply(Msg, State),
                    {error,
                        emqx_coap_message:set(block1, {Num, More, blockwise_size(State)}, Reply),
                        State};
                false ->
                    do_handle_server_block1(Num, More, Size, Msg, PeerKey, State)
            end
    end;
handle_server_block1(_, Msg, _PeerKey, State) ->
    {error, error_reply({error, bad_option}, Msg), State}.

do_handle_server_block1(Num, More, Size, Msg, PeerKey, State) ->
    Key = server_block1_key(PeerKey, Msg),
    ServerMap = maps:get(server_rx_block1, State),
    case {Num, maps:get(Key, ServerMap, undefined)} of
        {0, _Prev} ->
            handle_first_server_block1(More, Size, Msg, Key, ServerMap, State);
        {_Num, undefined} ->
            {error, error_reply({error, request_entity_incomplete}, Msg), State};
        {_Num, Tx = #{next_num := Expected, size := TxSize, payload := Acc}} ->
            handle_next_server_block1(Num, More, Size, Msg, Key, Tx, Expected, TxSize, Acc, State)
    end.

handle_first_server_block1(More, Size, Msg, Key, ServerMap, State) ->
    Payload = Msg#coap_message.payload,
    Total = byte_size(Payload),
    case
        Total > max_body_size(State) orelse Total > max_total_size(State) orelse
            size1_too_large(Msg, State)
    of
        true ->
            {error, too_large_block1_reply(Msg, State), State};
        false when not More ->
            {complete, clear_block1(Msg), State#{
                server_rx_block1 => maps:remove(Key, ServerMap)
            }};
        false ->
            Tx = #{
                payload => Payload,
                next_num => 1,
                size => Size,
                req => Msg,
                expires_at => expires_at(State)
            },
            case try_put(server_rx_block1, Key, Tx, State) of
                {ok, State1} ->
                    {continue, continue_reply(Msg, 0, Size), State1};
                {error, too_many_exchanges} ->
                    {error, busy_block1_reply(Msg), State};
                {error, total_size_exhausted} ->
                    {error, too_large_block1_reply(Msg, State), State}
            end
    end.

handle_next_server_block1(Num, More, Size, Msg, Key, Tx, Expected, TxSize, Acc, State) ->
    ServerMap = maps:get(server_rx_block1, State),
    case Num =:= Expected andalso Size =:= TxSize of
        false ->
            {error, error_reply({error, request_entity_incomplete}, Msg), State#{
                server_rx_block1 => maps:remove(Key, ServerMap)
            }};
        true ->
            Payload = Msg#coap_message.payload,
            NewPayload = <<Acc/binary, Payload/binary>>,
            append_server_block1(Num, More, Size, Msg, Key, Tx, NewPayload, State)
    end.

append_server_block1(Num, More, Size, Msg, Key, Tx, NewPayload, State) ->
    ServerMap = maps:get(server_rx_block1, State),
    case byte_size(NewPayload) > max_body_size(State) of
        true ->
            {error, too_large_block1_reply(Msg, State), State#{
                server_rx_block1 => maps:remove(Key, ServerMap)
            }};
        false ->
            Tx2 = Tx#{
                payload => NewPayload,
                next_num => Num + 1,
                expires_at => expires_at(State)
            },
            case try_put(server_rx_block1, Key, Tx2, State) of
                {ok, State1} when More ->
                    {continue, continue_reply(Msg, Num, Size), State1};
                {ok, State1} ->
                    Full = clear_block1(Msg#coap_message{payload = NewPayload}),
                    {complete, Full, State1#{
                        server_rx_block1 => maps:remove(
                            Key, maps:get(server_rx_block1, State1)
                        )
                    }};
                {error, _Reason} ->
                    {error, too_large_block1_reply(Msg, State), State#{
                        server_rx_block1 => maps:remove(Key, ServerMap)
                    }}
            end
    end.

size1_too_large(Msg, State) ->
    case emqx_coap_message:get_option(size1, Msg, undefined) of
        Size when is_integer(Size) -> Size > min(max_body_size(State), max_total_size(State));
        _ -> false
    end.

busy_block1_reply(Msg) ->
    emqx_coap_message:set(max_age, 1, error_reply({error, too_many_requests}, Msg)).

too_large_block1_reply(Msg, State) ->
    emqx_coap_message:set(
        size1,
        min(max_body_size(State), max_total_size(State)),
        error_reply({error, request_entity_too_large}, Msg)
    ).

handle_client_tx_block1(Ctx, Resp, Key, Tx, State) ->
    Method = Resp#coap_message.method,
    case {Method, negotiated_block1_size(Resp, Tx), maps:get(size, Tx)} of
        {{ok, continue}, Size, OldSize} ->
            Offset = maps:get(next_num, Tx) * OldSize,
            send_next_client_tx_block(Ctx, Tx#{size => Size, next_num => Offset div Size}, State);
        {{error, request_entity_too_large}, Size, OldSize} when Size < OldSize ->
            send_next_client_tx_block(Ctx, Tx#{size => Size, next_num => 0}, State);
        _ ->
            State2 = State#{
                client_tx_block1 => maps:remove(Key, maps:get(client_tx_block1, State))
            },
            maybe_handle_rx_block2(Ctx, Resp, State2)
    end.

negotiated_block1_size(Resp, Tx) ->
    CurrentSize = maps:get(size, Tx),
    case emqx_coap_message:get_option(block1, Resp, undefined) of
        {_Num, _More, Size} when is_integer(Size), Size >= 16, Size < CurrentSize ->
            case is_valid_block_size(Size) of
                true -> Size;
                false -> CurrentSize
            end;
        _ ->
            CurrentSize
    end.

send_next_client_tx_block(Ctx, Tx, State) ->
    Payload = maps:get(payload, Tx),
    Req = maps:get(req, Tx),
    Size = maps:get(size, Tx),
    Num = maps:get(next_num, Tx),
    Offset = Num * Size,
    case Offset < byte_size(Payload) of
        true ->
            BlockReq = emqx_coap_message:set_payload_block(Payload, block1, {Num, true, Size}, Req),
            {Num, _More, _} = emqx_coap_message:get_option(block1, BlockReq, undefined),
            Tx2 = Tx#{next_num => Num + 1, expires_at => expires_at(State)},
            {send_next, BlockReq, put_client_tx(client_tx_key(Ctx), Tx2, State)};
        false ->
            Reply = error_reply({error, request_entity_incomplete}, Req),
            {deliver, Reply, clear_client_exchange(Ctx, State)}
    end.

maybe_handle_rx_block2(Ctx, Resp, State) ->
    case emqx_coap_message:get_option(block2, Resp, undefined) of
        undefined ->
            {deliver, Resp, clear_client_request(Ctx, State)};
        {Num, More, Size} when
            is_integer(Num), Num >= 0, is_boolean(More), is_integer(Size)
        ->
            case is_valid_block_size(Size) andalso Size =< blockwise_size(State) of
                true ->
                    handle_client_rx_block2(Ctx, Num, More, Size, Resp, State);
                false ->
                    {deliver, Resp, clear_client_exchange(Ctx, State)}
            end;
        _ ->
            {deliver, Resp, clear_client_exchange(Ctx, State)}
    end.

handle_client_rx_block2(Ctx, Num, More, Size, Resp, State) ->
    Key = client_rx_key(Ctx),
    RxMap = maps:get(client_rx_block2, State),
    case {Num, maps:get(Key, RxMap, undefined)} of
        {0, _} ->
            rx_block2_start(Ctx, Key, More, Size, Resp, RxMap, State);
        {_Num, undefined} ->
            ?SLOG(warning, #{
                msg => "coap_rx_block2_unexpected_mid_sequence",
                block_num => Num
            }),
            {deliver, Resp, clear_client_request(Ctx, State)};
        {_Num, Rx} ->
            rx_block2_append(Ctx, Key, Num, More, Size, Resp, Rx, RxMap, State)
    end.

rx_block2_start(Ctx, Key, More, Size, Resp, RxMap, State) ->
    Payload = Resp#coap_message.payload,
    case byte_size(Payload) > max_body_size(State) of
        true ->
            rx_block2_abort(Ctx, Key, Resp, RxMap, State, body_too_large);
        false when More ->
            NextReq = next_block2_request(Ctx, Resp, 1, Size, State),
            Rx = #{
                chunks => [Payload],
                total_size => byte_size(Payload),
                next_num => 1,
                size => Size,
                expires_at => expires_at(State)
            },
            case try_put(client_rx_block2, Key, Rx, State) of
                {ok, State1} ->
                    {send_next, NextReq, State1};
                {error, _Reason} ->
                    rx_block2_abort(Ctx, Key, Resp, RxMap, State, resource_limit)
            end;
        false ->
            State1 = State#{client_rx_block2 => maps:remove(Key, RxMap)},
            {deliver, clear_block2(Resp), clear_client_request(Ctx, State1)}
    end.

rx_block2_append(Ctx, Key, Num, More, Size, Resp, Rx, RxMap, State) ->
    #{next_num := Expected, size := RxSize, chunks := Chunks, total_size := TotalSize} = Rx,
    Payload = Resp#coap_message.payload,
    case Num =:= Expected andalso Size =:= RxSize of
        false ->
            ?SLOG(warning, #{
                msg => "coap_rx_block2_sequence_mismatch",
                expected_num => Expected,
                got_num => Num,
                expected_size => RxSize,
                got_size => Size
            }),
            rx_block2_abort(Ctx, Key, Resp, RxMap, State, sequence_mismatch);
        true ->
            NewTotal = TotalSize + byte_size(Payload),
            case NewTotal > max_body_size(State) of
                true ->
                    rx_block2_abort(Ctx, Key, Resp, RxMap, State, body_too_large);
                false ->
                    Rx2 = Rx#{
                        chunks => [Payload | Chunks],
                        total_size => NewTotal,
                        next_num => Num + 1,
                        expires_at => expires_at(State)
                    },
                    case try_put(client_rx_block2, Key, Rx2, State) of
                        {ok, State1} when More ->
                            NextReq = next_block2_request(Ctx, Resp, Num + 1, Size, State1),
                            {send_next, NextReq, State1};
                        {ok, State1} ->
                            FullPayload = iolist_to_binary(lists:reverse([Payload | Chunks])),
                            Full = clear_block2(Resp#coap_message{payload = FullPayload}),
                            State2 = State1#{
                                client_rx_block2 => maps:remove(
                                    Key, maps:get(client_rx_block2, State1)
                                )
                            },
                            {deliver, Full, clear_client_request(Ctx, State2)};
                        {error, _Reason} ->
                            rx_block2_abort(Ctx, Key, Resp, RxMap, State, resource_limit)
                    end
            end
    end.

rx_block2_abort(Ctx, Key, Resp, RxMap, State, Reason) ->
    ?SLOG(warning, #{
        msg => "coap_rx_block2_aborted",
        reason => Reason
    }),
    State1 = State#{client_rx_block2 => maps:remove(Key, RxMap)},
    {deliver, block2_abort_reply(Resp, Reason), clear_client_request(Ctx, State1)}.

block2_abort_reply(Resp = #coap_message{}, body_too_large) ->
    Resp#coap_message{method = {error, request_entity_too_large}};
block2_abort_reply(Resp = #coap_message{}, sequence_mismatch) ->
    Resp#coap_message{method = {error, request_entity_incomplete}};
block2_abort_reply(Resp = #coap_message{}, resource_limit) ->
    Resp#coap_message{method = {error, service_unavailable}}.

next_block2_request(Ctx, Resp, Num, Size, State) ->
    Req0 = block2_template(Ctx, Resp, State),
    Req1 = clear_block1(Req0),
    Token =
        case Resp#coap_message.token of
            <<>> -> Req1#coap_message.token;
            T -> T
        end,
    Req2 = Req1#coap_message{payload = <<>>, token = Token},
    emqx_coap_message:set(block2, {Num, false, Size}, Req2).

block2_template(Ctx, Resp, State) ->
    case Ctx of
        #{request := Req0} when is_record(Req0, coap_message) ->
            Req0;
        _ ->
            ReqMap = maps:get(client_req, State),
            case maps:get(client_req_key(Ctx), ReqMap, undefined) of
                #{request := Req} when is_record(Req, coap_message) ->
                    Req;
                _ ->
                    ?SLOG(warning, #{
                        msg => "coap_block2_template_missing_request",
                        hint => "using response as template, block2 continuation may be incorrect"
                    }),
                    Resp
            end
    end.

server_block1_key(PeerKey, #coap_message{method = Method, token = Token, options = Opts}) ->
    {
        PeerKey,
        Method,
        maps:get(uri_path, Opts, undefined),
        maps:get(uri_query, Opts, undefined),
        Token
    }.

server_tx_key(PeerKey, Req, _Reply) when is_record(Req, coap_message) ->
    server_tx_key_from_req(PeerKey, Req);
server_tx_key(PeerKey, _Req, #coap_message{token = Token}) when Token =/= <<>> ->
    {server_tx_block2, PeerKey, Token};
server_tx_key(_PeerKey, _Req, _Reply) ->
    {error, no_token}.

server_tx_key_from_req(PeerKey, #coap_message{token = Token}) when Token =/= <<>> ->
    {server_tx_block2, PeerKey, Token};
server_tx_key_from_req(_PeerKey, _) ->
    {error, no_token}.

client_tx_key(Ctx) ->
    {client_tx_block1, normalize_ctx(Ctx)}.

client_rx_key(Ctx) ->
    {client_rx_block2, normalize_ctx(Ctx)}.

client_req_key(Ctx) ->
    {client_req, normalize_ctx(Ctx)}.

normalize_ctx(Ctx) when is_map(Ctx) ->
    maps:without([request], Ctx);
normalize_ctx(Ctx) ->
    Ctx.

clear_block1(#coap_message{options = Opts} = Msg) ->
    Msg#coap_message{options = maps:remove(block1, Opts)}.

clear_block2(#coap_message{options = Opts} = Msg) ->
    Msg#coap_message{options = maps:remove(block2, Opts)}.

continue_reply(Msg, Num, Size) ->
    Reply = emqx_coap_message:piggyback({ok, continue}, Msg),
    emqx_coap_message:set(block1, {Num, true, Size}, Reply).

error_reply(Method, Msg) ->
    emqx_coap_message:piggyback(Method, Msg).

put_client_tx(Key, Tx, State) ->
    Map = maps:get(client_tx_block1, State),
    State#{client_tx_block1 => Map#{Key => Tx}}.

put_client_request(Ctx, Req, State) ->
    Key = client_req_key(Ctx),
    ReqItem = #{request => Req, expires_at => expires_at(State)},
    ReqMap = maps:get(client_req, State),
    State#{client_req => ReqMap#{Key => ReqItem}}.

clear_client_request(Ctx, State) ->
    Key = client_req_key(Ctx),
    ReqMap = maps:get(client_req, State),
    State#{client_req => maps:remove(Key, ReqMap)}.

clear_client_tx(Ctx, State) ->
    Key = client_tx_key(Ctx),
    TxMap = maps:get(client_tx_block1, State),
    State#{client_tx_block1 => maps:remove(Key, TxMap)}.

clear_client_rx(Ctx, State) ->
    Key = client_rx_key(Ctx),
    RxMap = maps:get(client_rx_block2, State),
    State#{client_rx_block2 => maps:remove(Key, RxMap)}.

clear_client_exchange(Ctx, State0) ->
    State1 = clear_client_tx(Ctx, State0),
    State2 = clear_client_rx(Ctx, State1),
    clear_client_request(Ctx, State2).

maybe_expire(State) ->
    case has_blockwise_entries(State) of
        true -> expire(erlang:monotonic_time(millisecond), State);
        false -> State
    end.

has_blockwise_entries(#{
    server_rx_block1 := ServerRx,
    server_tx_block2 := ServerTx,
    client_tx_block1 := ClientTx,
    client_rx_block2 := ClientRx,
    client_req := ClientReq
}) ->
    map_size(ServerRx) > 0 orelse
        map_size(ServerTx) > 0 orelse
        map_size(ClientTx) > 0 orelse
        map_size(ClientRx) > 0 orelse
        map_size(ClientReq) > 0.

expires_at(State) ->
    erlang:monotonic_time(millisecond) + maps:get(exchange_lifetime, maps:get(opts, State)).

normalize_opts(Opts0) ->
    OptValues = [
        {enable, maps:get(enable, Opts0, true)},
        {max_block_size,
            normalize_block_size(maps:get(max_block_size, Opts0, ?DEFAULT_MAX_BLOCK_SIZE))},
        {max_body_size,
            normalize_max_body_size(maps:get(max_body_size, Opts0, ?DEFAULT_MAX_BODY_SIZE))},
        {max_concurrent_exchanges,
            normalize_positive_integer(
                max_concurrent_exchanges,
                maps:get(
                    max_concurrent_exchanges, Opts0, ?DEFAULT_MAX_CONCURRENT_EXCHANGES
                ),
                ?DEFAULT_MAX_CONCURRENT_EXCHANGES
            )},
        {max_total_size,
            normalize_positive_integer(
                max_total_size,
                maps:get(max_total_size, Opts0, ?DEFAULT_MAX_TOTAL_SIZE),
                ?DEFAULT_MAX_TOTAL_SIZE
            )},
        {exchange_lifetime,
            normalize_exchange_lifetime(
                maps:get(exchange_lifetime, Opts0, ?DEFAULT_EXCHANGE_LIFETIME)
            )}
    ],
    lists:foldl(fun({Key, Value}, Acc) -> put_if_defined(Acc, Key, Value) end, #{}, OptValues).

put_if_defined(Map, _Key, undefined) ->
    Map;
put_if_defined(Map, Key, Value) ->
    Map#{Key => Value}.

normalize_block_size(Size) when is_integer(Size) ->
    case is_valid_block_size(Size) of
        true ->
            Size;
        false ->
            ?SLOG(warning, #{
                msg => "coap_blockwise_invalid_block_size",
                configured => Size,
                using_default => ?DEFAULT_MAX_BLOCK_SIZE
            }),
            ?DEFAULT_MAX_BLOCK_SIZE
    end;
normalize_block_size(Other) ->
    ?SLOG(warning, #{
        msg => "coap_blockwise_invalid_block_size",
        configured => Other,
        using_default => ?DEFAULT_MAX_BLOCK_SIZE
    }),
    ?DEFAULT_MAX_BLOCK_SIZE.

normalize_max_body_size(MaxBody) when is_integer(MaxBody), MaxBody > 0 ->
    MaxBody;
normalize_max_body_size(Other) ->
    ?SLOG(warning, #{
        msg => "coap_blockwise_invalid_max_body_size",
        configured => Other,
        using_default => ?DEFAULT_MAX_BODY_SIZE
    }),
    ?DEFAULT_MAX_BODY_SIZE.

normalize_exchange_lifetime(T) when is_integer(T), T > 0 ->
    T;
normalize_exchange_lifetime(Other) ->
    ?SLOG(warning, #{
        msg => "coap_blockwise_invalid_exchange_lifetime",
        configured => Other,
        using_default => ?DEFAULT_EXCHANGE_LIFETIME
    }),
    ?DEFAULT_EXCHANGE_LIFETIME.

normalize_positive_integer(_Name, Value, _Default) when is_integer(Value), Value > 0 ->
    Value;
normalize_positive_integer(Name, Value, Default) ->
    ?SLOG(warning, #{
        msg => "coap_blockwise_invalid_limit",
        limit => Name,
        configured => Value,
        using_default => Default
    }),
    Default.

try_put(MapName, Key, Value, State) ->
    Map = maps:get(MapName, State),
    Candidate = State#{MapName => Map#{Key => Value}},
    case admission(Candidate) of
        ok -> {ok, Candidate};
        {error, Reason} -> {error, Reason}
    end.

admission(#{opts := Opts} = State) ->
    {ExchangeCount, TotalSize} = resource_usage(State),
    case
        ExchangeCount > maps:get(max_concurrent_exchanges, Opts, ?DEFAULT_MAX_CONCURRENT_EXCHANGES)
    of
        true ->
            {error, too_many_exchanges};
        false ->
            case TotalSize > max_total_size(State) of
                true -> {error, total_size_exhausted};
                false -> ok
            end
    end.

max_total_size(#{opts := Opts}) ->
    maps:get(max_total_size, Opts, ?DEFAULT_MAX_TOTAL_SIZE).

resource_usage(State) ->
    ServerRx = maps:get(server_rx_block1, State),
    ServerTx = maps:get(server_tx_block2, State),
    ClientContexts = client_contexts(State),
    ExchangeCount = map_size(ServerRx) + map_size(ServerTx) + length(ClientContexts),
    TotalSize =
        map_payload_size(ServerRx, payload) +
            map_payload_size(ServerTx, payload) +
            lists:sum([client_context_size(Ctx, State) || Ctx <- ClientContexts]),
    {ExchangeCount, TotalSize}.

client_contexts(State) ->
    lists:usort(
        [Ctx || {_Type, Ctx} <- maps:keys(maps:get(client_tx_block1, State))] ++
            [Ctx || {_Type, Ctx} <- maps:keys(maps:get(client_rx_block2, State))]
    ).

client_context_size(Ctx, State) ->
    ReqSize = request_item_size(maps:get(client_req_key(Ctx), maps:get(client_req, State), #{})),
    TxSize = item_binary_size(
        maps:get(client_tx_key(Ctx), maps:get(client_tx_block1, State), #{}), payload
    ),
    RxSize = maps:get(
        total_size,
        maps:get(client_rx_key(Ctx), maps:get(client_rx_block2, State), #{}),
        0
    ),
    max(ReqSize, TxSize) + RxSize.

request_item_size(#{request := #coap_message{payload = Payload}}) ->
    byte_size(Payload);
request_item_size(_) ->
    0.

map_payload_size(Map, Field) ->
    maps:fold(fun(_Key, Item, Acc) -> Acc + item_binary_size(Item, Field) end, 0, Map).

item_binary_size(Item, Field) ->
    case maps:get(Field, Item, <<>>) of
        Value when is_binary(Value) -> byte_size(Value);
        _ -> 0
    end.

is_valid_block_size(Size) when is_integer(Size) ->
    Size >= 16 andalso Size =< ?MAX_BLOCK_SIZE andalso (Size band (Size - 1)) =:= 0.
