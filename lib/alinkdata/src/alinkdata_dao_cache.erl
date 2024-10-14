%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 07. 8月 2022 上午1:30
%%%-------------------------------------------------------------------
-module(alinkdata_dao_cache).
-author("yqfclid").

-behaviour(gen_server).

%% API
-export([
    load/0,
    load/1,
    load_from_mappers/1,
    get_dao/1,
    insert_dao/2,
    delete_dao/1,
    generate_tables_daos/0,
    is_generated_table/1
]).

-export([start_link/0]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-define(DAO_CACHE, ?MODULE).

-define(GENERATED_TABLES, generated_tables).

-record(state, {}).

%%%===================================================================
%%% API
%%%===================================================================
load() ->
    Path = code:priv_dir(alinkdata),
    MapperDirPath = filename:join([Path, "mapper"]),
    case filelib:is_dir(MapperDirPath) of
        true ->
            lists:foreach(
                fun(Name) ->
                    FileName = filename:join(MapperDirPath, Name),
                    load(FileName)
            end, filelib:wildcard("*.mapper", MapperDirPath));
        _ ->
            ignore
    end.

load(FileName) ->
    {ok, RawMappers} = file:consult(FileName),
    load_from_mappers(RawMappers).

load_from_mappers(RawMappers) ->
    lists:foreach(
        fun({DaoId, Dao}) ->
            ets:insert(?DAO_CACHE, {DaoId, Dao})
    end, RawMappers).




generate_tables_daos() ->
    Daos = alinkdata_generate:generate_tables_daos(),
    Tables =
        lists:foldl(
            fun({DaoId, Dao, Table}, Acc) ->
                ets:insert(?DAO_CACHE, {DaoId, Dao}),
                [Table|Acc]
        end, [], Daos),
    ets:insert(?DAO_CACHE, {?GENERATED_TABLES, lists:usort(Tables)}).

is_generated_table(Table) ->
    case ets:lookup(?DAO_CACHE, ?GENERATED_TABLES) of
        [{_, Tables}] -> lists:member(Table, Tables);
        _ -> false
    end.


get_dao(DaoId) ->
    case ets:lookup(?DAO_CACHE, DaoId) of
        [] ->
            {error, not_found};
        [{DaoId, Dao}] ->
            {ok, Dao};
        {error, Reason} ->
            {error, Reason}
    end.

insert_dao(DaoId, Dao) ->
    ets:insert(?DAO_CACHE, {DaoId, Dao}).

delete_dao(DaoId) ->
    ets:delete(?DAO_CACHE, DaoId).


start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    ets:new(?DAO_CACHE, [named_table, set, public, {read_concurrency, true}]),
    load(),
    generate_tables_daos(),
    {ok, #state{}}.

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State = #state{}) ->
    ok.

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
