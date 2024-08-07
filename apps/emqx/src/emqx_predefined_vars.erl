%%--------------------------------------------------------------------
%% Copyright (c) 2018-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_predefined_vars).

-feature(maybe_expr, enable).

-behaviour(gen_server).
-behaviour(emqx_db_backup).

-include("emqx.hrl").
-include("logger.hrl").
-include("types.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("hocon/include/hoconsc.hrl").

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    handle_continue/2,
    terminate/2,
    code_change/3
]).

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

-export([create_tables/0, backup_tables/0, tables/0]).

-export([start_link/0, stop/0]).

-export([add/1, delete/1, to_sensitive_map/1]).

-define(TAB, predefined_vars).

%% runtime table
-define(RT, predefined_vars_rt).

-type source_type() :: env | plain | builtin.
-type runtime_type() :: binary | int | float | map.

-record(?TAB, {
    name :: binary(),
    source_type :: source_type(),
    source_value :: binary(),
    %% this is a reserved field
    runtime_type = binary :: runtime_type(),
    sensitive = false :: boolean(),
    extra = #{} :: map()
}).

-record(?RT, {
    name :: binary(),
    value :: binary(),
    extra = #{} :: map()
}).

-define(UNDEFINED_ENV_VAL, <<"undefined">>).
-define(BUILT_IN_VARS, [
    <<"EMQX_LOG_DIR">>,
    <<"EMQX_ETC_DIR">>,
    <<"EMQX_DATA_DIR">>,
    <<"EMQX_BUILDER_VSN">>,
    <<"EMQX_DASHBOARD_VERSION">>
]).

-define(ERROR_MSG_UPDATE_BUILT, <<"Unable to update builtin">>).
-define(ERROR_MSG_DELETE_BUILT, <<"Unable to delete builtin">>).

-define(CALL_TIMEOUT, 10000).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

%%--------------------------------------------------------------------
%% schema
%%--------------------------------------------------------------------
namespace() ->
    ?TAB.

roots() ->
    [?TAB].

fields(?TAB) ->
    [
        {bootstrap,
            ?HOCON(
                ?ARRAY(?R_REF(item)),
                #{
                    desc => ?DESC(bootstrap)
                }
            )}
    ];
fields(item) ->
    [
        {name,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(name),
                    required => true
                }
            )},
        {source_type,
            ?HOCON(
                ?ENUM([env, plain]),
                #{
                    desc => ?DESC(source_type),
                    required => true
                }
            )},
        {source_value,
            ?HOCON(
                binary(),
                #{
                    desc => ?DESC(source_type),
                    required => true
                }
            )},
        {sensitive,
            ?HOCON(
                boolean(),
                #{
                    desc => ?DESC(sensitive),
                    default => false
                }
            )}
    ].

desc(?TAB) ->
    ?DESC(?TAB);
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start the banned server.
-spec start_link() -> startlink_ret().
start_link() ->
    _ = mria:wait_for_tables(create_tables()),
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% for tests
-spec stop() -> ok.
stop() -> gen_server:stop(?MODULE).

add(Data) ->
    call({?FUNCTION_NAME, Data}).

delete(Name) ->
    call({?FUNCTION_NAME, Name}).

to_sensitive_map(Data) ->
    to_map(Data, true).
%%--------------------------------------------------------------------
%% Table Maintain
%%--------------------------------------------------------------------

create_tables() ->
    Options = [
        {type, set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, ?TAB},
        {attributes, record_info(fields, ?TAB)},
        {storage_properties, [{ets, [{read_concurrency, true}]}]}
    ],
    ok = mria:create_table(?TAB, Options),
    [?TAB].

backup_tables() -> tables().

-spec tables() -> [atom()].
tables() -> [?TAB].

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = ets:new(?RT, [
        ordered_set,
        public,
        named_table,
        {keypos, #?RT.name},
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    {ok, #{}, {continue, init_from_bootstrap}}.

handle_continue(init_from_bootstrap, State) ->
    init_from_bootstrap(),
    {noreply, State, {continue, load_into_runtime}};
handle_continue(load_into_runtime, State) ->
    load_into_runtime(),
    {noreply, State}.

handle_call({add, Data}, _From, State) ->
    %% todo safe from map
    Result =
        case from_map(Data) of
            {ok, #?TAB{source_type = builtin}} ->
                {error, ?ERROR_MSG_UPDATE_BUILT};
            {ok, Record} ->
                trans(fun() ->
                    case mnesia:read(?TAB, Record#?TAB.name) of
                        [#?TAB{source_type = builtin}] ->
                            mnesia:abort(?ERROR_MSG_UPDATE_BUILT);
                        _ ->
                            mnesia:write(Record)
                    end
                end);
            Error ->
                Error
        end,
    {reply, Result, State};
handle_call({delete, Name}, _From, State) ->
    Result = trans(fun() ->
        case mnesia:read(?TAB, Name) of
            [#?TAB{source_type = builtin}] ->
                mnesia:abort(?ERROR_MSG_DELETE_BUILT);
            [_] ->
                mnesia:delete({?TAB, Name});
            _ ->
                ok
        end
    end),
    {reply, Result, State};
handle_call(Req, _From, State) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    {reply, ignored, State}.

handle_cast(Msg, State) ->
    ?SLOG(error, #{msg => "unexpected_msg", cast => Msg}),
    {noreply, State}.

handle_info({mnesia_table_event, {write, Record, _}}, State) ->
    on_var_update(Record),
    {noreply, State};
handle_info({mnesia_table_event, {delete_object, Record, _}}, State) ->
    on_var_deleted(Record),
    {noreply, State};
handle_info({mnesia_table_event, {delete, {?TAB, Key}, _}}, State) ->
    on_var_deleted(Key),
    {noreply, State};
handle_info(Info, State) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {noreply, State}.

terminate(_Reason, _State) ->
    _ = mnesia:unsubscribe({table, ?TAB, simple}),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
init_from_bootstrap() ->
    maybe
        core ?= mria_rlog:role(),
        '$end_of_table' ?= mnesia:dirty_first(?TAB),
        Bootstrap0 = emqx:get_config([?TAB, bootstrap], []),
        Bootstrap = append_builtin_vars(Bootstrap0),
        {ok, _} ?= load_from_bootstrap(Bootstrap),
        ok
    else
        replicant ->
            ok;
        Name when is_binary(Name) ->
            ok;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "init_predefined_vars_failed",
                reason => Reason
            })
    end.

append_builtin_vars(Bootstrap) ->
    lists:foldl(
        fun(Env, Acc) ->
            [
                #{
                    name => Env,
                    source_type => builtin,
                    source_value => Env,
                    sensitive => false
                }
                | Acc
            ]
        end,
        Bootstrap,
        ?BUILT_IN_VARS
    ).

load_from_bootstrap(Bootstrap) ->
    Records =
        lists:foldl(
            fun(Map, Acc) ->
                case from_map(Map) of
                    {ok, Record} ->
                        [Record | Acc];
                    _ ->
                        ?SLOG(info, #{
                            msg => "load_predefined_vars_bootstrap_error",
                            data => Map
                        })
                end
            end,
            [],
            Bootstrap
        ),
    Fun = fun() ->
        lists:foreach(fun mnesia:write/1, Records)
    end,
    trans(Fun).

load_into_runtime() ->
    {ok, _} = mnesia:subscribe({table, ?TAB, simple}),
    Vars = mnesia:dirty_select(?TAB, ets:fun2ms(fun(Item) -> Item end)),
    load_into_runtime(Vars).

load_into_runtime([]) ->
    ok;
load_into_runtime(Vars) ->
    RTs = lists:map(fun source_to_rt/1, Vars),
    ets:insert(?RT, RTs).

source_to_rt(#?TAB{
    name = Name,
    source_type = plain,
    source_value = Value
}) ->
    #?RT{name = Name, value = Value};
source_to_rt(#?TAB{
    name = Name,
    source_type = _Type,
    source_value = SrcVal
}) ->
    #?RT{
        name = Name,
        value = get_env(SrcVal)
    }.

get_env(Env) ->
    get_env(Env, ?UNDEFINED_ENV_VAL).

get_env(Env, Default) ->
    case os:getenv(emqx_utils_conv:str(Env)) of
        false ->
            Default;
        Val ->
            emqx_utils_conv:bin(Val)
    end.

on_var_update(Src) ->
    RT = source_to_rt(Src),
    ets:insert(?RT, RT).

on_var_deleted(#?TAB{name = Name}) ->
    ets:delete(?RT, Name);
on_var_deleted(Key) ->
    ets:delete(?RT, Key).

from_map(
    #{
        name := Name,
        source_type := SrcType,
        source_value := SrcVal
    } = Data
) ->
    Sensitive = maps:get(sensitive, Data, false),
    {ok, #?TAB{
        name = Name,
        source_type = SrcType,
        source_value = SrcVal,
        sensitive = Sensitive
    }};
from_map(#{<<"name">> := _} = Map) ->
    try
        Data = emqx_utils_maps:safe_atom_key_map(Map),
        from_map(Data)
    catch
        _:Reason ->
            {error, Reason}
    end;
from_map(Data) ->
    {error, {invalid_data, Data}}.

to_map(
    #?TAB{
        name = Name,
        source_type = SrcType,
        source_value = SrcVal,
        sensitive = Sensitive
    },
    Redact
) ->
    Render = fun
        (true, true, _Value) -> emqx_utils_redact:redact_value();
        (_, _, Value) -> Value
    end,
    #{
        name => Name,
        source_type => SrcType,
        source_value => Render(Redact, Sensitive, SrcVal),
        sensitive => Sensitive
    }.

%% get_runtime_value(Name) ->
%%     case ets:lookup(?RT, Name) of
%%         [#?RT{value = Value}] ->
%%             Value;
%%         _ ->
%%             undefined
%%     end.

call(Msg) ->
    gen_server:call(?MODULE, Msg, ?CALL_TIMEOUT).

trans(Fun) ->
    case mria:transaction(?COMMON_SHARD, Fun) of
        {atomic, Res} -> {ok, Res};
        {aborted, Reason} -> {error, Reason}
    end.

%% trans(Fun, Args) ->
%%     case mria:transaction(?COMMON_SHARD, Fun, Args) of
%%         {atomic, Res} -> {ok, Res};
%%         {aborted, Reason} -> {error, Reason}
%%     end.
