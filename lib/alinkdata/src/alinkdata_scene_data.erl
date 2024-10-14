%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2024, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 9月 2024 下午3:29
%%%-------------------------------------------------------------------
-module(alinkdata_scene_data).
-author("yqfclid").

%% API
-export([
    add_hook/0,
    scene_data/4
]).


-export([
    start_link/0,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-export([
    delete_scene/1,
    lookup_or_create/1,
    add_scene/2
]).

-record(state, {}).

-define(CHECK_INTERVAL, 5 * 60).

%%%===================================================================
%%% API
%%%===================================================================
add_hook() ->
    alinkdata_hooks:add('data.scene_publish', {?MODULE, scene_data, []}).

scene_data(SceneId, ProductId, Addr, Data) ->
    QueryData = #{<<"id">> => SceneId},
    case alinkdata_dao:query_no_count('QUERY_scene', QueryData) of
        {ok, [#{<<"category">> := <<"rxwyj">>}]} ->
            Pid = lookup_or_create(SceneId),
            Pid ! {data, ProductId, Addr, Data};
        {ok, _} ->
            skip;
        {error, Reason} ->
            logger:error("get scene error ~p ~p", [SceneId, Reason])
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================_
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    ets:new(?MODULE, [named_table, set, public]),
    add_hook(),
    erlang:send_after(?CHECK_INTERVAL, self(), check_if_delete),
    {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.

handle_info(check_if_delete, State) ->
    check_if_delete(),
    erlang:send_after(?CHECK_INTERVAL, self(), check_if_delete),
    {noreply, State};
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.


delete_scene(Obj) ->
    ets:delete_object(?MODULE, Obj).

add_scene(SceneId, Pid) ->
    ets:insert(?MODULE, {SceneId, Pid}).

lookup_or_create(SceneId) ->
    case ets:lookup(?MODULE, SceneId) of
        [] ->
            {ok, Pid} = alinkdata_scene_worker:start(SceneId),
            Pid;
        [{_, Pid}] ->
            Pid
    end.


check_if_delete() ->
        case alinkdata_dao:query_no_count('QUERY_scene', #{}) of
            {ok, RaweScenes} ->
                Scenes =
                    lists:map(
                        fun(#{<<"id">> := Id}) -> Id
                    end, RaweScenes),
                lists:foreach(
                    fun({SceneId, Pid}) ->
                        case lists:member(SceneId, Scenes) of
                            true ->
                                skip;
                            _ ->
                                gen_server:cast(Pid, stop)
                        end
                end, ets:tab2list(?MODULE));
            {error, Reason} ->
                logger:error("scene error ~p ", [Reason])
        end.


