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

-export([make_iterator/3, store/5, next/1]).

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

-type cf_refs() :: [{_CFName :: string(), _CFRef :: reference()}].

-record(generation, {
    %% Module that handles data for the generation
    module :: module(),
    %% Module-specific attributes
    data :: term()
    %    time_range :: {emqx_replay:time(), emqx_replay:time()}
}).

-record(s, {
    zone :: emqx_types:zone(),
    db :: rocksdb:db_handle(),
    column_families :: cf_refs()
}).

-record(it, {
    module :: module(),
    data :: term()
}).

-type gen_id() :: 0..16#ffff.

-opaque iterator() :: #it{}.

%% Contents of the default column family:
%%
%% [{<<"genNN">>, #generation{}}, ...,
%%  {<<"current">>, GenID}]

-define(DEFAULT_CF_OPTS, []).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(emqx_types:zone()) -> {ok, pid()}.
start_link(Zone) ->
    gen_server:start_link(?MODULE, [Zone], []).

-spec make_iterator(emqx_types:zone(), emqx_topic:words(), emqx_replay_message_storage:time()) ->
    {ok, _TODO} | {error, _TODO}.
make_iterator(Zone, TopicFilter, StartTime) ->
    %% TODO: this is not supposed to work like this. Just a mock-up
    #generation{module = Mod, data = Data} = meta_lookup(Zone, 0),
    case Mod:make_iterator(Data, TopicFilter, StartTime) of
        {ok, It} ->
            {ok, #it{
                module = Mod,
                data = It
            }};
        Err ->
            Err
    end.

-spec store(emqx_types:zone(), emqx_guid:guid(), emqx_replay:time(), emqx_replay:topic(), binary()) ->
    ok | {error, _TODO}.
store(Zone, GUID, Time, Topic, Msg) ->
    %% TODO: this is not supposed to work like this. Just a mock-up
    #generation{module = Mod, data = Data} = meta_lookup(Zone, 0),
    Mod:store(Data, GUID, Time, Topic, Msg).

-spec next(iterator()) -> {value, binary(), iterator()} | none | {error, closed}.
next(#it{module = Mod, data = It0}) ->
    case Mod:next(It0) of
        {value, Val, It} ->
            {value, Val, #it{module = Mod, data = It}};
        Other ->
            Other
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

-spec read_metadata(#s{}) -> #s{}.
read_metadata(S) ->
    %% TODO: just a mockup to make the existing tests pass
    read_metadata(0, S).

-spec read_metadata(gen_id(), #s{}) -> #s{}.
read_metadata(GenId, S = #s{zone = Zone, db = DBHandle, column_families = CFs}) ->
    Gen = #generation{module = Mod, data = Data} = schema_get_gen(DBHandle, GenId),
    DB = Mod:open(DBHandle, GenId, CFs, Data),
    meta_put(Zone, GenId, Gen#generation{data = DB}).

-spec ensure_current_generation(#s{}) -> #s{}.
ensure_current_generation(S = #s{zone = Zone, db = DBHandle, column_families = CFs}) ->
    case schema_get_current(DBHandle) of
        undefined ->
            GenId = 0,
            ok = schema_put_current(DBHandle, GenId),
            create_new_generation_schema(GenId, S);
        _GenId ->
            S
    end.

-spec create_new_generation_schema(gen_id(), #s{}) -> #s{}.
create_new_generation_schema(
    GenId, S = #s{zone = Zone, db = DBHandle, column_families = CFs}
) ->
    {Module, Options} = emqx_replay_conf:zone_config(Zone),
    {NewGenData, NewCFs} = Module:create_new(DBHandle, GenId, Options),
    NewGen = #generation{
        module = Module,
        data = NewGenData
    },
    %% TODO: Transaction? Column family creation can't be transactional, anyway.
    ok = schema_put_gen(DBHandle, GenId, NewGen),
    S#s{column_families = NewCFs ++ CFs}.

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

%% Functions for dealing with the metadata stored persistently in rocksdb

-define(CURRENT_GEN, <<"current">>).
-define(SCHEMA_WRITE_OPTS, []).
-define(SCHEMA_READ_OPTS, []).

-spec schema_get_gen(rocksdb:db_handle(), gen_id()) -> #generation{}.
schema_get_gen(DBHandle, GenId) ->
    {ok, Bin} = rocksdb:get(DBHandle, gen_rocksdb_key(GenId), ?SCHEMA_READ_OPTS),
    binary_to_term(Bin).

-spec schema_put_gen(rocksdb:db_handle(), gen_id(), #generation{}) -> ok | {error, _}.
schema_put_gen(DBHandle, GenId, Gen) ->
    rocksdb:put(DBHandle, gen_rocksdb_key(GenId), term_to_binary(Gen), ?SCHEMA_WRITE_OPTS).

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

-spec gen_rocksdb_key(integer()) -> string().
gen_rocksdb_key(N) ->
    <<"gen", N:32>>.

-undef(CURRENT_GEN).
-undef(SCHEMA_WRITE_OPTS).
-undef(SCHEMA_READ_OPTS).

%% Functions for dealing with the runtime zone metadata:

-define(PERSISTENT_TERM(ZONE, GEN), {?MODULE, ZONE, GEN}).

-spec meta_lookup(emqx_types:zone(), gen_id()) -> #generation{}.
meta_lookup(Zone, GenId) ->
    persistent_term:get(?PERSISTENT_TERM(Zone, GenId)).

-spec meta_put(emqx_types:zone(), gen_id(), #generation{}) -> ok.
meta_put(Zone, GenId, Gen) ->
    persistent_term:put(?PERSISTENT_TERM(Zone, GenId), Gen).

-spec meta_erase(emqx_types:zone()) -> ok.
meta_erase(Zone) ->
    [
        persistent_term:erase(K)
     || {K = ?PERSISTENT_TERM(Z, _), _} <- persistent_term:get(), Z =:= Zone
    ],
    ok.

-undef(PERSISTENT_TERM).

%% -spec store_cfs(rocksdb:db_handle(), [{string(), rocksdb:cf_handle()}]) -> ok.
%% store_cfs(DBHandle, CFRefs) ->
%%     lists:foreach(
%%       fun({CFName, CFRef}) ->
%%               persistent_term:put({self(), CFName}, {DBHandle, CFRef})
%%       end,
%%       CFRefs).
