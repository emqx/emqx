%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader_store).

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-export([
    open/0,
    close/0
]).

-export([
    %% Leadership claims
    claim_leadership/3,
    renew_leadership/3,
    disown_leadership/2,
    %% Accessors
    leader_id/1,
    alive_until/1,
    heartbeat_interval/1
]).

-type group() :: binary().
-type leader_claim(ID) :: {ID, _Heartbeat :: emqx_message:timestamp()}.

-define(DS_DB, dqleader).

-define(LEADER_TTL, 30_000).
-define(LEADER_HEARTBEAT_INTERVAL, 10_000).

-define(LEADER_TOPIC_PREFIX, <<"$leader">>).
-define(LEADER_HEADER_HEARTBEAT, <<"$leader.ts">>).

%%

open() ->
    emqx_ds:open_db(?DS_DB, db_config()).

close() ->
    emqx_ds:close_db(?DS_DB).

db_config() ->
    Config = emqx_ds_schema:db_config([durable_storage, queues]),
    Config#{
        atomic_batches => true,
        force_monotonic_timestamps => false
    }.

%%

-spec claim_leadership(group(), ID, emqx_message:timestamp()) ->
    {ok | exists, leader_claim(ID)} | emqx_ds:error(_).
claim_leadership(Group, LeaderID, TS) ->
    LeaderClaim = {LeaderID, TS},
    case try_replace_leader(Group, LeaderClaim, undefined) of
        ok ->
            {ok, LeaderClaim};
        {exists, ExistingClaim = {_, LastHeartbeat}} when LastHeartbeat > TS - ?LEADER_TTL ->
            {exists, ExistingClaim};
        {exists, ExistingClaim = {_LeaderDead, _}} ->
            case try_replace_leader(Group, LeaderClaim, ExistingClaim) of
                ok ->
                    {ok, LeaderClaim};
                {exists, ConcurrentClaim} ->
                    {exists, ConcurrentClaim};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec renew_leadership(group(), leader_claim(ID), emqx_message:timestamp()) ->
    {ok | exists, leader_claim(ID)} | emqx_ds:error(_).
renew_leadership(Group, LeaderClaim = {LeaderID, LastHeartbeat}, TS) ->
    RenewedClaim = {LeaderID, TS},
    IsRenewable = (LastHeartbeat > TS - ?LEADER_TTL),
    case IsRenewable andalso try_replace_leader(Group, RenewedClaim, LeaderClaim) of
        ok ->
            {ok, RenewedClaim};
        {exists, NewestClaim} ->
            {exists, NewestClaim};
        false ->
            {error, unrecoverable, claim_outdated};
        Error ->
            Error
    end.

-spec disown_leadership(group(), leader_claim(_ID)) ->
    ok | emqx_ds:error(_).
disown_leadership(Group, LeaderClaim) ->
    try_delete_leader(Group, LeaderClaim).

-spec leader_id(leader_claim(ID)) ->
    ID.
leader_id({LeaderID, _}) ->
    LeaderID.

-spec alive_until(leader_claim(_)) ->
    emqx_message:timestamp().
alive_until({_LeaderID, LastHeartbeatTS}) ->
    LastHeartbeatTS + ?LEADER_TTL.

-spec heartbeat_interval(leader_claim(_)) ->
    _Milliseconds :: pos_integer().
heartbeat_interval(_) ->
    ?LEADER_HEARTBEAT_INTERVAL.

%%

try_replace_leader(Group, LeaderClaim, ExistingClaim) ->
    ds_store_batch(#dsbatch{
        preconditions = [mk_precondition(Group, ExistingClaim)],
        operations = [encode_leader_claim(Group, LeaderClaim)]
    }).

try_delete_leader(Group, LeaderClaim) ->
    {_Cond, Matcher} = mk_precondition(Group, LeaderClaim),
    ds_store_batch(#dsbatch{operations = [{delete, Matcher}]}).

ds_store_batch(Batch) ->
    case emqx_ds:store_batch(?DS_DB, Batch) of
        ok ->
            ok;
        {error, unrecoverable, {precondition_failed, Mismatch}} ->
            {exists, decode_leader_msg(Mismatch)};
        Error ->
            Error
    end.

mk_precondition(Group, undefined) ->
    {unless_exists, #message_matcher{
        from = Group,
        topic = mk_leader_topic(Group),
        timestamp = 0,
        payload = '_'
    }};
mk_precondition(Group, {Leader, HeartbeatTS}) ->
    {if_exists, #message_matcher{
        from = Group,
        topic = mk_leader_topic(Group),
        timestamp = 0,
        payload = encode_leader(Leader),
        headers = #{?LEADER_HEADER_HEARTBEAT => HeartbeatTS}
    }}.

encode_leader_claim(Group, {Leader, HeartbeatTS}) ->
    #message{
        id = <<>>,
        qos = 0,
        from = Group,
        topic = mk_leader_topic(Group),
        timestamp = 0,
        payload = encode_leader(Leader),
        headers = #{?LEADER_HEADER_HEARTBEAT => HeartbeatTS}
    }.

decode_leader_msg(#message{from = _Group, payload = Payload, headers = Headers}) ->
    Leader = decode_leader(Payload),
    Heartbeat = maps:get(?LEADER_HEADER_HEARTBEAT, Headers, 0),
    {Leader, Heartbeat}.

encode_leader(Leader) ->
    %% NOTE: Lists are compact but easy to extend later.
    term_to_binary([Leader]).

decode_leader(Payload) ->
    [Leader | _Extra] = binary_to_term(Payload),
    Leader.

mk_leader_topic(GroupName) ->
    emqx_topic:join([?LEADER_TOPIC_PREFIX, GroupName]).
