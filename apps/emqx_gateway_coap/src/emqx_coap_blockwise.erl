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
    enabled/1
]).

-export_type([state/0]).

-include("emqx_coap.hrl").
-include_lib("emqx/include/logger.hrl").

-define(DEFAULT_MAX_BLOCK_SIZE, 1024).
-define(DEFAULT_MAX_BODY_SIZE, 4 * 1024 * 1024).
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
        exchange_lifetime => ?DEFAULT_EXCHANGE_LIFETIME
    }.

-spec enabled(state()) -> boolean().
enabled(#{opts := Opts}) -> maps:get(enable, Opts, true).

-spec blockwise_size(state()) -> pos_integer().
blockwise_size(#{opts := Opts}) -> maps:get(max_block_size, Opts, ?DEFAULT_MAX_BLOCK_SIZE).

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
    | {error, coap_message(), state()}.
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
    {single, coap_message(), state()} | {first_block, coap_message(), state()}.
client_prepare_out_request(Ctx, Msg, State0) ->
    State = maybe_expire(State0),
    State1 = put_client_request(Ctx, Msg, State),
    case block1_required(Msg, State) of
        false ->
            {single, Msg, State1};
        true ->
            Size = blockwise_size(State),
            Payload = Msg#coap_message.payload,
            Key = client_tx_key(Ctx),
            First = emqx_coap_message:set_payload_block(Payload, block1, {0, true, Size}, Msg),
            Tx = #{
                payload => Payload,
                size => Size,
                next_num => 1,
                req => Msg,
                expires_at => expires_at(State)
            },
            {first_block, First, put_client_tx(Key, Tx, State1)}
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
            State1 = put_server_tx(Key, Tx, More, State),
            ?SLOG(debug, #{
                msg => "coap_block2_tx_chunked",
                key => Key,
                block_num => Num,
                block_size => Size,
                more => More
            }),
            {chunked, ChunkedReply, State1};
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
    case is_valid_block_size(Size) andalso Size =< blockwise_size(State) of
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
        #{size := TxSize} when TxSize =/= Size ->
            ?SLOG(warning, #{
                msg => "coap_block2_followup_size_mismatch",
                key => Key,
                expected_size => TxSize,
                got_size => Size
            }),
            State1 = State#{server_tx_block2 => maps:remove(Key, TxMap)},
            {error, error_reply({error, bad_option}, Msg), State1};
        Tx ->
            case build_server_tx_block(Msg, undefined, Num, Tx) of
                {ok, Reply, More} ->
                    Tx2 = Tx#{expires_at => expires_at(State)},
                    State1 = put_server_tx(Key, Tx2, More, State),
                    ?SLOG(debug, #{
                        msg => "coap_block2_followup_served",
                        key => Key,
                        block_num => Num,
                        block_size => Size,
                        more => More
                    }),
                    {reply, Reply, State1};
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
            case is_valid_block_size(Size) andalso Size =< DefaultSize of
                true -> {ok, Num, Size};
                false -> error
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

reply_request(#coap_message{} = Req, _Reply) -> Req.

maybe_clear_server_tx(PeerKey, Req, State) ->
    case server_tx_key_from_req(PeerKey, Req) of
        {error, no_token} ->
            State;
        Key ->
            TxMap = maps:get(server_tx_block2, State),
            State#{server_tx_block2 => maps:remove(Key, TxMap)}
    end.

put_server_tx(Key, _Tx, false, State) ->
    TxMap = maps:get(server_tx_block2, State),
    State#{server_tx_block2 => maps:remove(Key, TxMap)};
put_server_tx(Key, Tx, true, State) ->
    TxMap = maps:get(server_tx_block2, State),
    Observe = maps:get(observe, Tx, undefined),
    TxMap2 =
        case maps:get(Key, TxMap, undefined) of
            #{observe := Observe} = Prev when Observe =/= undefined ->
                TxMap#{Key => Prev#{expires_at => maps:get(expires_at, Tx)}};
            _ ->
                TxMap#{Key => Tx}
        end,
    State#{server_tx_block2 => TxMap2}.

handle_server_block1({Num, More, Size}, Msg, PeerKey, State) when
    is_integer(Num), Num >= 0, is_boolean(More), is_integer(Size)
->
    case is_valid_block_size(Size) andalso Size =< blockwise_size(State) of
        false ->
            {error, error_reply({error, bad_option}, Msg), State};
        true ->
            do_handle_server_block1(Num, More, Size, Msg, PeerKey, State)
    end;
handle_server_block1(_, Msg, _PeerKey, State) ->
    {error, error_reply({error, bad_option}, Msg), State}.

do_handle_server_block1(Num, More, Size, Msg, PeerKey, State) ->
    Key = server_block1_key(PeerKey, Msg),
    ServerMap = maps:get(server_rx_block1, State),
    MaxBody = max_body_size(State),
    Payload = Msg#coap_message.payload,
    case {Num, maps:get(Key, ServerMap, undefined)} of
        {0, _Prev} ->
            Total = byte_size(Payload),
            case Total > MaxBody of
                true ->
                    {error, error_reply({error, request_entity_too_large}, Msg), State};
                false ->
                    case More of
                        true ->
                            Tx = #{
                                payload => Payload,
                                next_num => 1,
                                size => Size,
                                req => Msg,
                                expires_at => expires_at(State)
                            },
                            Continue = continue_reply(Msg, Num, Size),
                            {continue, Continue, State#{server_rx_block1 => ServerMap#{Key => Tx}}};
                        false ->
                            {complete, clear_block1(Msg), State#{
                                server_rx_block1 => maps:remove(Key, ServerMap)
                            }}
                    end
            end;
        {_Num, undefined} ->
            {error, error_reply({error, request_entity_incomplete}, Msg), State};
        {_Num, Tx = #{next_num := Expected, size := TxSize, payload := Acc}} ->
            case Num =:= Expected andalso Size =:= TxSize of
                false ->
                    {
                        error,
                        error_reply({error, request_entity_incomplete}, Msg),
                        State#{server_rx_block1 => maps:remove(Key, ServerMap)}
                    };
                true ->
                    NewPayload = <<Acc/binary, Payload/binary>>,
                    case byte_size(NewPayload) > MaxBody of
                        true ->
                            {
                                error,
                                error_reply({error, request_entity_too_large}, Msg),
                                State#{server_rx_block1 => maps:remove(Key, ServerMap)}
                            };
                        false when More ->
                            Tx2 = Tx#{
                                payload => NewPayload,
                                next_num => Expected + 1,
                                expires_at => expires_at(State)
                            },
                            Continue = continue_reply(Msg, Num, Size),
                            {
                                continue,
                                Continue,
                                State#{server_rx_block1 => ServerMap#{Key => Tx2}}
                            };
                        false ->
                            Full = clear_block1(Msg#coap_message{payload = NewPayload}),
                            {
                                complete,
                                Full,
                                State#{server_rx_block1 => maps:remove(Key, ServerMap)}
                            }
                    end
            end
    end.

handle_client_tx_block1(Ctx, Resp, Key, Tx, State) ->
    Method = Resp#coap_message.method,
    case Method of
        {ok, continue} ->
            send_next_client_tx_block(Ctx, Tx, State);
        _ ->
            State2 = State#{
                client_tx_block1 => maps:remove(Key, maps:get(client_tx_block1, State))
            },
            maybe_handle_rx_block2(Ctx, Resp, State2)
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
            {send_next, NextReq, State#{client_rx_block2 => RxMap#{Key => Rx}}};
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
                false when More ->
                    NextReq = next_block2_request(Ctx, Resp, Num + 1, Size, State),
                    Rx2 = Rx#{
                        chunks => [Payload | Chunks],
                        total_size => NewTotal,
                        next_num => Num + 1,
                        expires_at => expires_at(State)
                    },
                    {send_next, NextReq, State#{client_rx_block2 => RxMap#{Key => Rx2}}};
                false ->
                    FullPayload = iolist_to_binary(lists:reverse([Payload | Chunks])),
                    Full = clear_block2(Resp#coap_message{payload = FullPayload}),
                    State1 = State#{client_rx_block2 => maps:remove(Key, RxMap)},
                    {deliver, Full, clear_client_request(Ctx, State1)}
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
block2_abort_reply(Resp = #coap_message{}, _Reason) ->
    Resp#coap_message{method = {error, bad_request}}.

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
    ReqMap = maps:get(client_req, State),
    ReqItem = #{request => Req, expires_at => expires_at(State)},
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

is_valid_block_size(Size) when is_integer(Size) ->
    Size >= 16 andalso Size =< ?MAX_BLOCK_SIZE andalso (Size band (Size - 1)) =:= 0.
