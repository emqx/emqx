%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 8月 2023 下午1:00
%%%-------------------------------------------------------------------
-module(alinkcore_lanzun_watcher).

-behaviour(gen_server).

-export([
    start_link/0
]).


-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-define(SERVER, ?MODULE).

-record(state, {
    start_watch = false,
    addrs = []
}).

-include("alinkcore.hrl").

-define(APPLICATION_EHCK_INTERVAL, 10000).
-define(LANZUN_GNSS_CHECK_INTERVAL, 60000 * 5).
%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


%%%===================================================================
%%% Spawning and gen_server implementation
%%%===================================================================
init([]) ->
    erlang:send_after(?APPLICATION_EHCK_INTERVAL, self(), check_application),
    erlang:send_after(?LANZUN_GNSS_CHECK_INTERVAL, self(), lanzun_product_check),
    {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.



handle_info(lanzun_product_check, #state{start_watch = true, addrs = Addrs} = State) ->
    NAddrs =
        case get_latest_lanzun_device() of
            {ok, Devices} ->
                DeviceAddrs = lists:map(fun(#{<<"addr">> := A}) -> A end, Devices),
                DeviceAddrOpts = lists:map(fun(#{<<"addr">> := A, <<"product">> := P}) -> {A, P} end, Devices),
                lists:foreach(
                    fun(Stoped) ->
                        case alinkcore_cache:lookup(session, Stoped) of
                            undefined ->
                                ok;
                            #session{pid = Pid} ->
                                lanzun_gnss_client:stop(Pid)
                        end
                end, Addrs -- DeviceAddrs),
                lists:foreach(
                    fun(Add) ->
                        PId = proplists:get_value(Add, DeviceAddrOpts, 0),
                        case alinkcore_cache:lookup(session, Add) of
                            undefined ->
                                {ok, #{<<"config">> := ConfigRaw}} = alinkcore_cache:query_device(Add),
                                DeviceId = maps:get(<<"device_id">>, ConfigRaw, <<>>),
                                lanzun_gnss_client:start(PId, Add, DeviceId);
                            #session{pid = _} ->
                                ok
                        end
                end, DeviceAddrs -- Addrs),
                DeviceAddrs;
            {error, Reason} ->
                logger:error("query lanzun product device error:~p", [Reason]),
                Addrs
        end,
    erlang:send_after(?LANZUN_GNSS_CHECK_INTERVAL, self(), lanzun_product_check),
    {noreply, State#state{addrs = NAddrs}};
handle_info(lanzun_product_check, State) ->
    erlang:send_after(?LANZUN_GNSS_CHECK_INTERVAL, self(), lanzun_product_check),
    {noreply, State};
handle_info(check_application, State) ->
    case is_applications_ok() of
        true ->
            {noreply, State#state{start_watch = true}};
        _ ->
            erlang:send_after(?APPLICATION_EHCK_INTERVAL, self(), check_application),
            {noreply, State}
    end;
handle_info(_Info, State = #state{}) ->
    {noreply, State}.

terminate(_Reason, _State = #state{}) ->
    ok.

code_change(_OldVsn, State = #state{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
is_applications_ok() ->
    Applications = [alinkiot, alinkdata, alinkcore, alinkalarm, alinkiot_tdengine],
    RunningApplications = proplists:get_value(running, application:info()),
    lists:all(fun(App) -> lists:keyfind(App, 1, RunningApplications) =/= false end, Applications).

get_latest_lanzun_device() ->
    Sql = <<"select a.product as product,a.addr as addr from sys_device a, sys_product b ",
            "where a.product = b.id and b.protocol = 'LANZUN-GNSS'">>,
    case alinkdata:query_mysql_format_map(default, Sql) of
        {ok, Devices} ->
            {ok, Devices};
        {error, Reason} ->
            {error, Reason}
    end.
