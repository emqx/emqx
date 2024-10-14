%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 28. 5月 2023 下午3:08
%%%-------------------------------------------------------------------
-module(alinkdata_collect_service).

%% API
-export([
    stats_info/4,
    metrics_device/4,
    metrics_alarm/4
]).

%%%===================================================================
%%% API
%%%===================================================================
stats_info(_OperationID, _Args, _Context, Req) ->
    {ok, [#{<<"count(id)">> := DeviceCount}]} =
        alinkdata:query_mysql_format_map(default, <<"select count(id) from sys_device">>),
    {ok, [#{<<"count(id)">> := DeviceActiveCount}]} =
        alinkdata:query_mysql_format_map(default, <<"select count(id) from sys_device where status = '0'">>),
    {ok, [#{<<"count(id)">> := ProjectCount}]} =
        alinkdata:query_mysql_format_map(default, <<"select count(id) from sys_project">>),

    OnlineCount = alinkcore_cache:sessions_count(),
    {ok, AlarmStats} =
        alinkdata:query_mysql_format_map(default, <<"select count(id) as count, level from sys_alarm group by level">>),
    {ok, RAreas} = alinkdata:query_mysql_format_map(default, <<"select count(id) as count,province from sys_device group by province">>),
    Areas =
        lists:filter(fun(#{<<"province">> := Province}) -> not lists:member(Province, [<<>>, undefined, null]) end, RAreas),
    Stats =
        #{
            <<"device">> => #{
                <<"all_device">> => DeviceCount,
                <<"active_device">> => DeviceActiveCount,
                <<"online_device">> => OnlineCount
            },
            <<"alarm">> => AlarmStats,
            <<"project">> => #{
                <<"all_project">> => ProjectCount
            },
            <<"area">> => Areas
        },
    alinkdata_common_service:response(alinkdata_ajax_result:success_result(Stats), Req).


metrics_device(_OperationID, #{<<"interval">> := Interval, <<"type">> := <<"diff">>} = Args, _Context, Req) ->
    TimeDurationSql = time_duration_sql(Args),
    IntervalB = alinkutil_type:to_binary(Interval),
    Sql = <<"select TIMEDIFF(time, 0, 1s) as time, diff(all_device) as all_device, ",
        "diff(online_device) as online_device, diff(active_device) as active_device from(select _wstart as time, ",
        "last(online_device) as online_device, last(active_device) as active_device, last(all_device) as all_device",
        " from metrics_device where 1 = 1 ", TimeDurationSql/binary, " interval(", IntervalB/binary, "s)) ">>,
    Res =
        case request_with_page(Sql, Args) of
            {ok, Rows} ->
                alinkdata_ajax_result:success_result(Rows);
            {ok, Count, Rows} ->
                (alinkdata_ajax_result:success_result())#{total => Count, rows => Rows}
        end,
    alinkdata_common_service:response(Res, Req);
metrics_device(_OperationID, #{<<"interval">> := Interval} = Args, _Context, Req) ->
    TimeDurationSql = time_duration_sql(Args),
    IntervalB = alinkutil_type:to_binary(Interval),
    Sql = <<"select _wstart as time, ",
        "last(online_device) as online_device, last(active_device) as active_device, last(all_device) as all_device",
        " from metrics_device where 1 = 1 ", TimeDurationSql/binary, " interval(", IntervalB/binary, "s) ">>,
    Res =
        case request_with_page(Sql, Args) of
            {ok, Rows} ->
                alinkdata_ajax_result:success_result(Rows);
            {ok, Count, Rows} ->
                (alinkdata_ajax_result:success_result())#{total => Count, rows => Rows}
        end,
    alinkdata_common_service:response(Res, Req).


metrics_alarm(_OperationID, #{<<"interval">> := Interval, <<"level">> := Level0, <<"type">> := <<"diff">>} = Args, _Context, Req) ->
    Level =
        case Level0 of
            <<"all">> ->
                -1;
            _ ->
                Level0
        end,
    TimeDurationSql = time_duration_sql(Args),
    IntervalB = alinkutil_type:to_binary(Interval),
    LevelB = alinkutil_type:to_binary(Level),
    Sql = <<"select time, diff(triged) from ",
        "(select TIMEDIFF(time, 0, 1s) as time, diff(triged) as triged from(select _wstart as time, ",
        "last(triged) as triged",
        " from metrics_alarm where level= ", LevelB/binary, " ", TimeDurationSql/binary, " interval(", IntervalB/binary, "s))) ">>,
    Res =
        case request_with_page(Sql, Args) of
            {ok, Rows} ->
                alinkdata_ajax_result:success_result(Rows);
            {ok, Count, Rows} ->
                (alinkdata_ajax_result:success_result())#{total => Count, rows => Rows}
        end,
    alinkdata_common_service:response(Res, Req);
metrics_alarm(_OperationID, #{<<"interval">> := Interval, <<"level">> := Level0} = Args, _Context, Req) ->
    Level =
        case Level0 of
            <<"all">> ->
                -1;
            _ ->
                Level0
        end,
    TimeDurationSql = time_duration_sql(Args),
    IntervalB = alinkutil_type:to_binary(Interval),
    LevelB = alinkutil_type:to_binary(Level),
    Sql = <<"select TIMEDIFF(time, 0, 1s) as time, diff(triged) as triged ",
        "from(select _wstart as time, ",
        "last(triged) as triged",
        " from metrics_alarm where level = ", LevelB/binary, " ", TimeDurationSql/binary, " interval(", IntervalB/binary, "s)) ">>,
    Res =
        case request_with_page(Sql, Args) of
            {ok, Rows} ->
                alinkdata_ajax_result:success_result(Rows);
            {ok, Count, Rows} ->
                (alinkdata_ajax_result:success_result())#{total => Count, rows => Rows}
        end,
    alinkdata_common_service:response(Res, Req).
%%%===================================================================
%%% Internal functions
%%%===================================================================
time_duration_sql(Args) ->
    BeginSql =
        case maps:get(<<"begin">>, Args, undefined) of
            undefined ->
                <<>>;
            Begin ->
                <<" and time >= '", Begin/binary, "'">>
        end,
    EndSql =
        case maps:get(<<"end">>, Args, undefined) of
            undefined ->
                <<>>;
            End ->
                <<" and time < '", End/binary, "'">>
        end,
    <<BeginSql/binary, EndSql/binary>>.



page_sql(#{<<"pageSize">> := RPageSize, <<"pageNum">> := RPageNum}) when RPageSize =/= undefined
                                                                    andalso RPageNum =/= undefined ->
    PageSize = alinkutil_type:to_integer(RPageSize),
    PageNum = alinkutil_type:to_integer(RPageNum),
    Offset = PageSize * (PageNum - 1),
    PageSizeB = alinkutil_type:to_binary(PageSize),
    OffsetB = alinkutil_type:to_binary(Offset),
    <<" LIMIT ", OffsetB/binary, ",", PageSizeB/binary>>;
page_sql(_) ->
    <<>>.



request_with_page(Sql, Agrs) ->
    PageSql = page_sql(Agrs),
    request(Sql, PageSql).



request(Sql, <<>>) ->
    case alinkiot_tdengine:request(Sql) of
        {ok, _, Rows} ->
            {ok, Rows};
        {error, Reason} ->
            logger:error("request failed ~p", [Reason]),
            {ok, []}
    end;
request(Sql, PageSql) ->
    case alinkiot_tdengine:request(<<"select count(1) from (", Sql/binary, ")">>) of
        {ok, 0, _} ->
            {ok, 0, []};
        {ok, _, [#{<<"count(1)">> := Count}]} ->
            case alinkiot_tdengine:request(<<Sql/binary, PageSql/binary>>) of
                {ok, _, Rows} ->
                    {ok, Count, Rows};
                {error, Reason} ->
                    logger:error("request failed ~p", [Reason]),
                    {ok, []}
            end;
        {error, #{<<"code">> := 9826,<<"desc">> := <<"Fail to get table info, error: Table does not exist">>}} ->
            {ok, 0, []};
        {error, Reason} ->
            logger:error("request failed ~p", [Reason]),
            {ok, []}
    end.
