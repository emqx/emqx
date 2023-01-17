%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_replay_local_store).

-behavior(gen_server).

%% API:
-export([start_link/1]).
-export([create_generation/3]).

-export([store/5, make_iterator/3, next/1]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([cf_refs/0, gen_id/0, db_write_options/0]).

-compile({inline, [meta_lookup/2]}).

%%================================================================================
%% Type declarations
%%================================================================================

%% see rocksdb:db_options()
-type options() :: proplists:proplist().

-type db_write_options() :: proplists:proplist().

-type cf_refs() :: [{string(), rocksdb:cf_handle()}].

%% Message storage generation
%% Keep in mind that instances of this type are persisted in long-term storage.
-type generation() :: #{
    %% Module that handles data for the generation
    module := module(),
    %% Module-specific data defined at generation creation time
    data := term(),
    %% When should this generation become active?
    %% This generation should only contain messages timestamped no earlier than that.
    %% The very first generation will have `since` equal 0.
    since := emqx_replay:time()
}.

-record(s, {
    zone :: emqx_types:zone(),
    db :: rocksdb:db_handle(),
    column_families :: cf_refs()
}).

-record(it, {
    zone :: emqx_types:zone(),
    gen :: gen_id(),
    filter :: emqx_topic:words(),
    start_time :: emqx_replay:time(),
    module :: module(),
    data :: term()
}).

-type gen_id() :: 0..16#ffff.

-opaque state() :: #s{}.
-opaque iterator() :: #it{}.

%% Contents of the default column family:
%%
%% [{<<"genNN">>, #generation{}}, ...,
%%  {<<"current">>, GenID}]

-define(DEFAULT_CF_OPTS, []).

-define(REF(Zone), {via, gproc, {n, l, {?MODULE, Zone}}}).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(emqx_types:zone()) -> {ok, pid()}.
start_link(Zone) ->
    gen_server:start_link(?REF(Zone), ?MODULE, [Zone], []).

-spec create_generation(emqx_types:zone(), emqx_replay:time(), emqx_replay_conf:backend_config()) ->
    {ok, gen_id()}.
create_generation(Zone, Since, Config = {_Module, _Options}) ->
    gen_server:call(?REF(Zone), {create_generation, Since, Config}).

-spec store(emqx_types:zone(), emqx_guid:guid(), emqx_replay:time(), emqx_replay:topic(), binary()) ->
    ok | {error, _TODO}.
store(Zone, GUID, Time, Topic, Msg) ->
    {_GenId, #{module := Mod, data := Data}} = meta_lookup_gen(Zone, Time),
    Mod:store(Data, GUID, Time, Topic, Msg).

-spec make_iterator(emqx_types:zone(), emqx_topic:words(), emqx_replay:time()) ->
    {ok, iterator()} | {error, _TODO}.
make_iterator(Zone, TopicFilter, StartTime) ->
    {GenId, Gen} = meta_lookup_gen(Zone, StartTime),
    open_iterator(Gen, #it{
        zone = Zone,
        gen = GenId,
        filter = TopicFilter,
        start_time = StartTime
    }).

-spec next(iterator()) -> {value, binary(), iterator()} | none | {error, closed}.
next(It = #it{module = Mod, data = ItData}) ->
    case Mod:next(ItData) of
        {value, Val, ItDataNext} ->
            {value, Val, It#it{data = ItDataNext}};
        {error, _} = Error ->
            Error;
        none ->
            case open_next_iterator(It) of
                {ok, ItNext} ->
                    next(ItNext);
                {error, _} = Error ->
                    Error;
                none ->
                    none
            end
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

init([Zone]) ->
    process_flag(trap_exit, true),
    {ok, DBHandle, CFRefs} = open_db(Zone),
    S0 = #s{
        zone = Zone,
        db = DBHandle,
        column_families = CFRefs
    },
    S = ensure_current_generation(S0),
    read_metadata(S),
    {ok, S}.

handle_call({create_generation, Since, Config}, _From, S) ->
    {ok, GenId, NS} = create_new_gen(Since, Config, S),
    {reply, {ok, GenId}, NS};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{db = DB, zone = Zone}) ->
    meta_erase(Zone),
    ok = rocksdb:close(DB).

%%================================================================================
%% Internal functions
%%================================================================================

-spec read_metadata(state()) -> ok.
read_metadata(S = #s{db = DBHandle}) ->
    Current = schema_get_current(DBHandle),
    lists:foreach(fun(GenId) -> read_metadata(GenId, S) end, lists:seq(0, Current)).

-spec read_metadata(gen_id(), state()) -> ok.
read_metadata(GenId, S = #s{zone = Zone, db = DBHandle}) ->
    Gen = open_gen(GenId, schema_get_gen(DBHandle, GenId), S),
    meta_register_gen(Zone, GenId, Gen).

-spec ensure_current_generation(state()) -> state().
ensure_current_generation(S = #s{zone = Zone, db = DBHandle}) ->
    case schema_get_current(DBHandle) of
        undefined ->
            Config = emqx_replay_conf:zone_config(Zone),
            {ok, _, NS} = create_new_gen(0, Config, S),
            NS;
        _GenId ->
            S
    end.

-spec create_new_gen(emqx_replay:time(), emqx_replay_conf:backend_config(), state()) ->
    {ok, gen_id(), state()}.
create_new_gen(Since, Config, S = #s{zone = Zone, db = DBHandle}) ->
    GenId = get_next_id(meta_get_current(Zone)),
    GenId = get_next_id(schema_get_current(DBHandle)),
    % TODO: Propagate errors to clients.
    true = is_gen_valid(Zone, GenId, Since),
    {ok, Gen, NS} = create_gen(GenId, Since, Config, S),
    %% TODO: Transaction? Column family creation can't be transactional, anyway.
    ok = schema_put_gen(DBHandle, GenId, Gen),
    ok = schema_put_current(DBHandle, GenId),
    ok = meta_register_gen(Zone, GenId, Gen),
    {ok, GenId, NS}.

% -spec
create_gen(GenId, Since, {Module, Options}, S = #s{db = DBHandle, column_families = CFs}) ->
    {Schema, NewCFs} = Module:create_new(DBHandle, GenId, Options),
    Gen = #{
        module => Module,
        data => Schema,
        since => Since
    },
    {ok, Gen, S#s{column_families = NewCFs ++ CFs}}.

-spec open_db(emqx_types:zone()) -> {ok, rocksdb:db_handle(), cf_refs()} | {error, _TODO}.
open_db(Zone) ->
    Filename = atom_to_list(Zone),
    DBOptions = emqx_replay_conf:db_options(),
    ColumnFamiles =
        case rocksdb:list_column_families(Filename, DBOptions) of
            {ok, ColumnFamiles0} ->
                [{I, []} || I <- ColumnFamiles0];
            % DB is not present. First start
            {error, {db_open, _}} ->
                [{"default", ?DEFAULT_CF_OPTS}]
        end,
    case rocksdb:open(Filename, [{create_if_missing, true} | DBOptions], ColumnFamiles) of
        {ok, Handle, CFRefs} ->
            {CFNames, _} = lists:unzip(ColumnFamiles),
            {ok, Handle, lists:zip(CFNames, CFRefs)};
        Error ->
            Error
    end.

-spec open_gen(gen_id(), generation(), state()) -> generation().
open_gen(
    GenId,
    Gen = #{module := Mod, data := Data},
    #s{zone = Zone, db = DBHandle, column_families = CFs}
) ->
    DB = Mod:open(Zone, DBHandle, GenId, CFs, Data),
    Gen#{data := DB}.

-spec open_next_iterator(iterator()) -> {ok, iterator()} | {error, _Reason} | none.
open_next_iterator(It = #it{zone = Zone, gen = GenId}) ->
    open_next_iterator(meta_get_gen(Zone, GenId + 1), It#it{gen = GenId + 1}).

open_next_iterator(undefined, _It) ->
    none;
open_next_iterator(Gen = #{}, It) ->
    open_iterator(Gen, It).

-spec open_iterator(generation(), iterator()) -> {ok, iterator()} | {error, _Reason}.
open_iterator(#{module := Mod, data := Data}, It = #it{}) ->
    case Mod:make_iterator(Data, It#it.filter, It#it.start_time) of
        {ok, ItData} ->
            {ok, It#it{module = Mod, data = ItData}};
        Err ->
            Err
    end.

%% Functions for dealing with the metadata stored persistently in rocksdb

-define(CURRENT_GEN, <<"current">>).
-define(SCHEMA_WRITE_OPTS, []).
-define(SCHEMA_READ_OPTS, []).

-spec schema_get_gen(rocksdb:db_handle(), gen_id()) -> generation().
schema_get_gen(DBHandle, GenId) ->
    {ok, Bin} = rocksdb:get(DBHandle, schema_gen_key(GenId), ?SCHEMA_READ_OPTS),
    binary_to_term(Bin).

-spec schema_put_gen(rocksdb:db_handle(), gen_id(), generation()) -> ok | {error, _}.
schema_put_gen(DBHandle, GenId, Gen) ->
    rocksdb:put(DBHandle, schema_gen_key(GenId), term_to_binary(Gen), ?SCHEMA_WRITE_OPTS).

-spec schema_get_current(rocksdb:db_handle()) -> gen_id() | undefined.
schema_get_current(DBHandle) ->
    case rocksdb:get(DBHandle, ?CURRENT_GEN, ?SCHEMA_READ_OPTS) of
        {ok, Bin} ->
            binary_to_integer(Bin);
        not_found ->
            undefined
    end.

-spec schema_put_current(rocksdb:db_handle(), gen_id()) -> ok | {error, _}.
schema_put_current(DBHandle, GenId) ->
    rocksdb:put(DBHandle, ?CURRENT_GEN, integer_to_binary(GenId), ?SCHEMA_WRITE_OPTS).

-spec schema_gen_key(integer()) -> binary().
schema_gen_key(N) ->
    <<"gen", N:32>>.

-undef(CURRENT_GEN).
-undef(SCHEMA_WRITE_OPTS).
-undef(SCHEMA_READ_OPTS).

%% Functions for dealing with the runtime zone metadata:

-define(PERSISTENT_TERM(ZONE, GEN), {?MODULE, ZONE, GEN}).

-spec meta_register_gen(emqx_types:zone(), gen_id(), generation()) -> ok.
meta_register_gen(Zone, GenId, Gen) ->
    Gs =
        case GenId > 0 of
            true -> meta_lookup(Zone, GenId - 1);
            false -> []
        end,
    ok = meta_put(Zone, GenId, [Gen | Gs]),
    ok = meta_put(Zone, current, GenId).

-spec meta_lookup_gen(emqx_types:zone(), emqx_replay:time()) -> {gen_id(), generation()}.
meta_lookup_gen(Zone, Time) ->
    % TODO
    % Is cheaper persistent term GC on update here worth extra lookup? I'm leaning
    % towards a "no".
    Current = meta_lookup(Zone, current),
    Gens = meta_lookup(Zone, Current),
    find_gen(Time, Current, Gens).

find_gen(Time, GenId, [Gen = #{since := Since} | _]) when Time >= Since ->
    {GenId, Gen};
find_gen(Time, GenId, [_Gen | Rest]) ->
    find_gen(Time, GenId - 1, Rest).

-spec meta_get_gen(emqx_types:zone(), gen_id()) -> generation() | undefined.
meta_get_gen(Zone, GenId) ->
    case meta_lookup(Zone, GenId, []) of
        [Gen | _Older] -> Gen;
        [] -> undefined
    end.

-spec meta_get_current(emqx_types:zone()) -> gen_id() | undefined.
meta_get_current(Zone) ->
    meta_lookup(Zone, current, undefined).

-spec meta_lookup(emqx_types:zone(), _K) -> _V.
meta_lookup(Zone, K) ->
    persistent_term:get(?PERSISTENT_TERM(Zone, K)).

-spec meta_lookup(emqx_types:zone(), _K, Default) -> _V | Default.
meta_lookup(Zone, K, Default) ->
    persistent_term:get(?PERSISTENT_TERM(Zone, K), Default).

-spec meta_put(emqx_types:zone(), _K, _V) -> ok.
meta_put(Zone, K, V) ->
    persistent_term:put(?PERSISTENT_TERM(Zone, K), V).

-spec meta_erase(emqx_types:zone()) -> ok.
meta_erase(Zone) ->
    [
        persistent_term:erase(K)
     || {K = ?PERSISTENT_TERM(Z, _), _} <- persistent_term:get(), Z =:= Zone
    ],
    ok.

-undef(PERSISTENT_TERM).

get_next_id(undefined) -> 0;
get_next_id(GenId) -> GenId + 1.

is_gen_valid(Zone, GenId, Since) when GenId > 0 ->
    [#{since := SincePrev} | _] = meta_lookup(Zone, GenId - 1),
    Since > SincePrev;
is_gen_valid(_Zone, 0, 0) ->
    true.

%% -spec store_cfs(rocksdb:db_handle(), [{string(), rocksdb:cf_handle()}]) -> ok.
%% store_cfs(DBHandle, CFRefs) ->
%%     lists:foreach(
%%       fun({CFName, CFRef}) ->
%%               persistent_term:put({self(), CFName}, {DBHandle, CFRef})
%%       end,
%%       CFRefs).
