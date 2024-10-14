%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 30. 8月 2023 下午1:00
%%%-------------------------------------------------------------------
-module(alinkcore_cehou_watcher).

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
-define(CEHOU_CHECK_INTERVAL, 60000 * 5).
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
    {ok, #state{}}.

handle_call(_Request, _From, State = #state{}) ->
    {reply, ok, State}.

handle_cast(_Request, State = #state{}) ->
    {noreply, State}.



handle_info(cehou_product_check, #state{start_watch = true, addrs = OAddrs} = State) ->
    NAddrs =
        case get_latest_cehou_device() of
            {ok, [#{<<"product">> := ProductId, <<"config">> := Config}|_] = Devices} ->
                Addrs = lists:map(fun(#{<<"addr">> := A}) -> A end, Devices),
                ConfigList = jiffy:decode(Config, [return_maps]),
                LastTime = get_last_time(ProductId),
                RawDatas = get_data(LastTime, ConfigList),
                deal_data(RawDatas, ProductId, Addrs),
                lists:foreach(
                    fun(Addr) ->
                        alinkcore_cache:delete(session, Addr)
                end, OAddrs -- Addrs),
                Addrs;
            {ok, []} ->
                OAddrs;
            {error, Reason} ->
                logger:error("query cehou product device error:~p", [Reason]),
                OAddrs
        end,
    erlang:send_after(?CEHOU_CHECK_INTERVAL, self(), cehou_product_check),
    {noreply, State#state{addrs = NAddrs}};
handle_info(cehou_product_check, State) ->
    erlang:send_after(?CEHOU_CHECK_INTERVAL, self(), cehou_product_check),
    {noreply, State};
handle_info(check_application, State) ->
    case is_applications_ok() of
        true ->
            handle_info(cehou_product_check, State#state{start_watch = true});
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

get_latest_cehou_device() ->
    Sql = <<"select a.product as product,a.addr as addr, b.config as config from sys_device a, sys_product b ",
        "where a.product = b.id and b.protocol = 'CEHOUSHEBEI'">>,
    case alinkdata:query_mysql_format_map(default, Sql) of
        {ok, Devices} ->
            {ok, Devices};
        {error, Reason} ->
            {error, Reason}
    end.


get_last_time(ProductId) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    Table = <<"alinkiot_", ProductIdB/binary>>,
    case alinkiot_tdengine:request(<<"describe ", Table/binary>>) of
        {ok, _, _} ->
            case alinkiot_tdengine:request(<<"select last(TIMEDIFF(time, 0, 1s)) as time from ", Table/binary>>) of
                {ok, 1, [#{<<"time">> := Time}]} ->
                    Time;
                {ok, 0, []} ->
                    0;
                {error, Reason} ->
                    logger:error("query td ~p cehou last time failed:~p", [ProductId, Reason]),
                    0
            end;
        {error, _Reason} ->
            0
    end.


get_data(StartTime, ConfigList) ->
    StartTimeBin = alinkdata_wechat:timestamp2localtime_str(StartTime),
    Host = find_key_from_config_list(<<"database_host">>, ConfigList, <<"127.0.0.1">>),
    Port = find_key_from_config_list(<<"database_port">>, ConfigList, 3306),
    Username = find_key_from_config_list(<<"database_user">>, ConfigList, "root"),
    Password = find_key_from_config_list(<<"database_password">>, ConfigList, "123456"),
    DataBaseOpts =
        [
            {host, alinkutil_type:to_list(Host)},
            {port, alinkutil_type:to_integer(Port)},
            {user, alinkutil_type:to_list(Username)},
            {password, alinkutil_type:to_list(Password)}
        ],
    case mysql:start_link(DataBaseOpts) of
        {ok, Pid} ->
            Sql = <<"SELECT jiedian,jiedianbm,shebei_status,dianliang_status,wendu,houdu,shangchuansj ",
                    "from changchuan.yw_jiedianjk where shangchuansj > '", StartTimeBin/binary, "' order by shangchuansj">>,
            case alinkdata:format_result_to_map(mysql:query(Pid, Sql)) of
                {ok, Datas} ->
                    mysql:stop(Pid),
                    Datas;
                {error, Reason} ->
                    logger:error("get cehou data failed:~p", [Reason]),
                    mysql:stop(Pid),
                    []
            end;
        {error, Reason} ->
            logger:error("cehou connect database failed:~p", [Reason])
    end.


deal_data(RawDatas, ProductId, Addrs) ->
    Fun =
        fun(RawData) ->
            #{
                <<"jiedian">> := JieDian,
                <<"shebei_status">> := ShebeiStatus,
                <<"dianliang_status">> := DianliangStatus,
                <<"wendu">> := Wendu,
                <<"houdu">> := Houdu,
                <<"shangchuansj">> := UpdateTime
            } = RawData,
            Addr = alinkutil_type:to_binary(JieDian),
            case lists:member(Addr, Addrs) of
                true ->
                    Ts = datetime2ts(UpdateTime),
                    case ShebeiStatus of
                        <<"1">> ->
                            alinkcore_cache:save(session, Addr, #session{pid = self(), node = node()});
                        _ ->
                            alinkcore_cache:delete(session, Addr)
                    end,
                    Data =
                        #{
                            <<"ts">> => Ts,
                            <<"wendu">> => #{<<"value">> => Wendu, <<"ts">> => Ts},
                            <<"houdu">> => #{<<"value">> => Houdu, <<"ts">> => Ts},
                            <<"shebei_status">> => #{<<"value">> => ShebeiStatus, <<"ts">> => Ts},
                            <<"dianliang_status">> => #{<<"value">> => DianliangStatus, <<"ts">> => Ts}
                        },
                    alinkcore_data:handle(ProductId, Addr, Data);
                _ ->
                    ok
            end
        end,
    lists:foreach(Fun, RawDatas).


find_key_from_config_list(_Name, [], Default) ->
    Default;
find_key_from_config_list(Name, [#{<<"name">> := Name, <<"value">> := Value}|_], _Default) ->
    Value;
find_key_from_config_list(Name, [_|T], Default) ->
    find_key_from_config_list(Name, T, Default).

datetime2ts({{YI, MonI, DI}, {HI, MI, SI}}) ->
    calendar:datetime_to_gregorian_seconds({{YI, MonI, DI}, {HI, MI, SI}}) - 3600 * 8 -
        calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).