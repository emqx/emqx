%%%-------------------------------------------------------------------
%%% @author kenneth
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 22. 四月 2020 16:38
%%%-------------------------------------------------------------------
-module(alinkiot_tdengine).
-author("kenneth").
-include("alinkiot_tdengine.hrl").
%% API
-export([
    init_alink/0,
    show_database/0,
    get_schema/1,
    create_database/2,
    create_schema/1,
    create_alink_schema/2,
    del_columns/2,
    del_column/2,
    add_columns/2,
    add_column/2,
    drop_stable/1,
    query_device_log/4,
    query_history/4,
    get_device_last_row/4,
    export_history/4,
    query_alink/1,

    insert_object/2,
    create_user/2,
    alter_user/2,
    delete_user/1,
    query_object/2,
    batch/1,
    request/1,
    do_query_history/1
]).

-define(DEFAULT_LIMIT, 30000).

%%%===================================================================
%%% API
%%%===================================================================
init_alink() ->
    case application:get_env(alinkiot_tdengine, database) of
        {ok, DB} ->
            create_database(DB, 3650);
        undefined ->
            ok
    end.


show_database() ->
    request(<<"SHOW DATABASES;">>).



create_database(DataBase, Keep) ->
    KeepTime = alinkutil_type:to_binary(Keep),
    DataBaseB = alinkutil_type:to_binary(DataBase),
    Sql = <<"CREATE DATABASE IF NOT EXISTS ", DataBaseB/binary, " KEEP ", KeepTime/binary>>,
    request(Sql).

get_schema(Table) ->
    Sql = <<"describe ", Table/binary>>,
    request(Sql).

%% tags => [#{name => tagname, type => tagtype},....]
create_schema(#{<<"table">> := _TableName, <<"using">> := _STbName, <<"tags">> := []}) ->
    {error, tags_empty};
create_schema(#{<<"table">> := TableName, <<"using">> := STbName, <<"tags">> := Tags}) ->
    TableNameB = alinkutil_type:to_binary(TableName),
    STbNameB = alinkutil_type:to_binary(STbName),
    Sql0 = <<"create table if not exists ", TableNameB/binary, " using ", STbNameB/binary, "(">>,
    {Sql1, Sql3} = create_subtable_fields_bin(Tags),
    Sql2 = <<") tags(">>,
    Sql4 = <<")">>,
    Sql = <<Sql0/binary, Sql1/binary, Sql2/binary, Sql3/binary, Sql4/binary>>,
    request(Sql);
create_schema(#{<<"fields">> := []}) ->
    {error, fields_empty};
create_schema(#{<<"stable">> := TableName, <<"fields">> := Fields} = Schema) ->
    TableNameB = alinkutil_type:to_binary(TableName),
    Sql0 = <<"create stable if not exists ", TableNameB/binary, " (time TIMESTAMP">>,
    Sql1 = create_table_fields_bin(Fields),
    Sql =
        case maps:get(<<"tags">>, Schema, []) of
            [] ->
                <<Sql0/binary, Sql1/binary, ")">>;
            Tags ->
                Sql2 = create_table_tags_bin(Tags),
                <<Sql0/binary, Sql1/binary, ") tags (", Sql2/binary, ")">>
        end,
    request(Sql);
create_schema(#{<<"table">> := TableName, <<"fields">> := Fields} = Schema) ->
    TableNameB = alinkutil_type:to_binary(TableName),
    Sql0 = <<"create table if not exists ", TableNameB/binary, " (time TIMESTAMP">>,
    Sql1 = create_table_fields_bin(Fields),
    Sql =
        case maps:get(<<"tags">>, Schema, []) of
            [] ->
                <<Sql0/binary, Sql1/binary, ")">>;
            Tags ->
                Sql2 = create_table_tags_bin(Tags),
                <<Sql0/binary, Sql1/binary, ") tags (", Sql2/binary, ")">>
        end,
    request(Sql).


create_alink_schema(ProductId, ThingInfo) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    STable = <<"alinkiot_", ProductIdB/binary>>,
    FilterThingInfo0 =
        lists:filter(
            fun(#{<<"name">> := Name}) ->
                Name =/= <<"time">>
        end, ThingInfo),
    FilterThingInfo1 =
        lists:map(
            fun(#{<<"name">> := Name} = FInfo) ->
                FInfo#{
                    <<"name">> => alinkiot_tdengine_common:to_td_field_name(Name),
                    <<"type">> => alinkiot_tdengine_common:thing_field_precision_type(FInfo)
                }
            end, FilterThingInfo0) ++ [#{<<"name">> => <<"alarm_rule">>, <<"type">> => <<"string">>}],
    Tags = [
        #{<<"name">> => <<"addr">>, <<"type">> => <<"string">>},
        #{<<"name">> => <<"product_id">>, <<"type">> => <<"int">>}
    ],
    CreateSchemaParam =
        #{
            <<"stable">> => STable,
            <<"fields">> => FilterThingInfo1,
            <<"tags">> => Tags
        },
    create_schema(CreateSchemaParam).


add_columns(Table, Columns) ->
    lists:foreach(
        fun(Column) ->
            case add_column(Table, Column) of
                {ok, _, _} ->
                    ok;
                {error, Reason} ->
                    logger:error("add column ~p in table ~p failed ~p",
                        [Column, Table, Reason])
            end
        end, Columns).

add_column(Table, #{<<"name">> := Name, <<"type">> := Type}) ->
    FieldType = alinkiot_tdengine_common:transform_field_type(Type),
    Sql = <<"alter table ", Table/binary, " add column ", Name/binary, " ", FieldType/binary>>,
    request(Sql).


del_columns(Table, Columns) ->
    lists:foreach(
        fun(Column) ->
            case del_column(Table, Column) of
                {ok, _, _} ->
                    ok;
                {error, Reason} ->
                    logger:error("delete column ~p in table ~p failed ~p",
                        [Column, Table, Reason])
            end
    end, Columns).

del_column(Table, Column) ->
    Sql = <<"alter table ", Table/binary, " drop column ", Column/binary>>,
    request(Sql).


drop_stable(Table) ->
    Sql = <<"drop stable ", Table/binary>>,
    request(Sql).





%% 插入
insert_object(TableName, #{<<"values">> := Values0} = Object) ->
    Values = format_batch(Object#{ <<"tableName">> => TableName, <<"values">> => [Values0]}),
    Sql = <<"INSERT INTO ", Values/binary, ";">>,
    request(Sql).


%% 查询，
query_device_log(_OperationID, RArgs, _Context, Req) ->
    Where =
        maps:fold(
            fun(K, V, Acc) ->
                case lists:member(K, [<<"time[begin]">>, <<"time[end]">>, <<"addr">>]) of
                    true ->
                        Acc#{K => V};
                    _ ->
                        Acc
                end
        end, #{}, RArgs),
    Now = erlang:system_time(second),
    BeforeTenMin = Now - 10 * 60,
    Where0 =
        case maps:get(<<"time[begin]">>, Where, undefined) of
            undefined ->
                Where#{<<"time[begin]">> => timestamp2localtime_str(BeforeTenMin)};
            _ ->
                Where
        end,
    Where1 =
        case maps:get(<<"time[end]">>, Where0, undefined) of
            undefined ->
                Where0#{<<"time[end]">> => timestamp2localtime_str(Now)};
            _ ->
                Where0
        end,
    Table = <<"metrics_device_log">>,
    Fields =
        lists:map(
            fun(F) ->
                #{<<"name">> => F}
        end, [<<"data">>, <<"addr">>, <<"event">>, <<"reason">>, <<"result">>, <<"ip">>]),
    Args = RArgs#{<<"table">> => Table, <<"where">> => Where1, <<"fields">> => Fields},
    case query_alink(Args) of
        {ok, Count, Rows} ->
            CommonSuccessRes = alinkdata_ajax_result:success_result(),
            Res = maps:merge(#{total => Count, rows => Rows}, CommonSuccessRes),
            alinkdata_common_service:response(Res, Req);
        {ok, Rows} ->
            alinkdata_common_service:response(alinkdata_ajax_result:success_result(Rows), Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            alinkdata_common_service:response(alinkdata_ajax_result:error_result(Msg), Req)
    end.

%% 查询，
query_history(_OperationID, RArgs, _Context, Req) ->

    Args =
        case maps:get(<<"step">>, RArgs, undefined) of
            undefined ->
                ExtraFields = [#{<<"name">> => <<"alarm_rule">>, <<"as">> => <<"alarmrule">>}],
                RArgs#{<<"extra_fields">> => ExtraFields};
            _ ->
                RArgs
        end,
    case do_query_history(Args) of
        {ok, Count, Rows} ->
            CommonSuccessRes = alinkdata_ajax_result:success_result(),
            Res = maps:merge(#{total => Count, rows => return_format_time(RArgs, Rows)}, CommonSuccessRes),
            alinkdata_common_service:response(Res, Req);
        {ok, Rows} ->
            alinkdata_common_service:response(alinkdata_ajax_result:success_result(return_format_time(RArgs, Rows)), Req);
        {error, Reason} ->
            Msg = list_to_binary(io_lib:format("~p", [Reason])),
            logger:error(Msg),
            alinkdata_common_service:response(alinkdata_ajax_result:error_result(), Req)
    end.

export_history(_OperationID, _Args, _Context, Req) ->
    {ok, B, _Req2} = cowboy_req:read_urlencoded_body(Req),
    Args = maps:from_list(B),
    NArgs = lists:foldl(fun(K, Acc) -> maps:remove(K, Acc) end, Args, [<<"pageNum">>, <<"pageSize">>, <<"addr">>]),
    FileName = <<"device_history_data.xlsx"/utf8>>,
    FileNameH = <<"\"", FileName/binary, "\"">>,
    RAddrs = binary:split(maps:get(<<"addr">>, Args, <<>>), <<",">>, [global]),
    Addrs = lists:filter(fun(A) -> A =/= <<>> end, RAddrs),
    ExtraFields =
        [
            #{<<"name">> => <<"addr">>, <<"as">> => <<"addr">>}
        ],
    case export_history_check_addr(Addrs) of
        {ok, AddrFirst} ->
            Vs =
                case maps:get(<<"step">>, Args, undefined) of
                    undefined ->
                        {ok,Vs0} = do_query_history_by_stable(NArgs#{<<"addrFirst">> => AddrFirst, <<"addrs">> => Addrs, <<"extra_fields">> => ExtraFields}),
                        Vs0;
                    _ ->
                        lists:foldl(
                            fun(A, Acc) ->
                                {ok,Vs0} = do_query_history_by_stable(NArgs#{<<"addrFirst">> => AddrFirst, <<"addrs">> => [A]}),
                                Acc ++ lists:map(fun(D) -> D#{<<"addr">> => A} end, Vs0)
                        end, [], Addrs)
                 end,
            T = <<"device_data_", AddrFirst/binary>>,
            {ok, Content} = alinkdata_xlsx:to_xlsx_file_content(FileName, T, Vs),
            Headers = alinkdata_common:file_headers(Content, FileNameH),
            {200, Headers, Content};
        {error, Reason} ->
            logger:error("export ~p history failed:~p", [Addrs, Reason]),
            Res = alinkdata_ajax_result:error_result(<<"导出设备出错，请检查设备设置是否正常"/utf8>>),
            alinkdata_common_service:response(Res, Req)
    end.


get_device_last_row(_OperationID, #{<<"addr">> := AddrsBin, <<"batch">> := <<"true">>} = RArgs, _Context, Req) ->
    Addrs =
        lists:filter(
            fun(A) -> A =/= <<>>
        end, binary:split(AddrsBin, <<",">>, [global])),
    AddrRows =
        lists:map(
            fun(Addr) ->
                Args = #{<<"addr">> => Addr, <<"function">> => <<"last_row">>},
                (do_get_device_last_row(Args))#{<<"addr">> => Addr}
        end, Addrs),
    Res = alinkdata_ajax_result:success_result(return_format_time(RArgs, AddrRows)),
    alinkdata_common_service:response(Res, Req);
get_device_last_row(_OperationID, #{<<"addr">> := Addr} = RArgs, _Context, Req) ->
    Args = #{<<"addr">> => Addr, <<"function">> => <<"last_row">>},
    ExtraFields = [#{<<"name">> => <<"alarm_rule">>, <<"as">> => <<"alarmrule">>}],
    Res =
        case do_query_history(Args#{<<"extra_fields">> => ExtraFields}) of
            {ok, []} ->
                alinkdata_ajax_result:success_result(#{});
            {ok, [Row|_]} ->
                alinkdata_ajax_result:success_result(return_format_time_row(RArgs, Row));
            {error, Reason} ->
                logger:error("query device ~p last row failed", [Addr, Reason]),
                alinkdata_ajax_result:success_result(#{})
        end,
    alinkdata_common_service:response(Res, Req).

do_get_device_last_row( #{<<"addr">> := Addr} = Args) ->
    ExtraFields = [#{<<"name">> => <<"alarm_rule">>, <<"as">> => <<"alarmrule">>}],
    case do_query_history(Args#{<<"extra_fields">> => ExtraFields}) of
        {ok, []} ->
            #{};
        {ok, [Row|_]} ->
            Row;
        {error, Reason} ->
            logger:error("query device ~p last row failed", [Addr, Reason]),
            #{}
    end.

export_history_check_addr([]) ->
    {error, wrong_device};
export_history_check_addr([Addr]) ->
    {ok, Addr};
export_history_check_addr(Addrs) ->
    AddrFirst = hd(Addrs),
    Args = #{<<"addrFirst">> => AddrFirst, <<"addrs">> => Addrs},
    case alinkdata_dao:query_no_count(get_same_product_device, Args) of
        {ok, SelectedAddrs} ->
            SelectedAddrList = lists:map(fun(#{<<"addr">> := A}) -> A end, SelectedAddrs),
            case lists:usort(Addrs) =:= lists:usort(SelectedAddrList) of
                true ->
                    {ok, AddrFirst};
                false ->
                    {error, wrong_device}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


do_query_history_by_stable(#{<<"addrFirst">> := AddrFirst} = Args) ->
    case alinkcore_cache:query_device(AddrFirst) of
        {ok, #{<<"product">> := PId}} ->
            case alinkcore_cache:query_product(PId) of
                {ok, #{<<"thing">> := Thing}} when Thing =/= []
                    andalso is_list(Thing)->
                    ExtraFields = maps:get(<<"extra_fields">>, Args, []),
                    ThingInfo =
                        lists:map(
                            fun(#{<<"name">> := F} = FInfo) ->
                                TdF = alinkiot_tdengine_common:to_td_field_name(F),
                                FInfo#{<<"name">> => TdF, <<"as">> => TdF}
                            end, Thing) ++ ExtraFields,
                    ThingFields = lists:map(fun(#{<<"name">> := F}) -> F end, ThingInfo),
                    Where =
                        maps:fold(
                            fun(K, V, Acc) ->
                                LegalFields = [<<"createTime">>, <<"createTime[begin]">>, <<"createTime[end]">>,
                                    <<"time">>, <<"time[begin]">>, <<"time[end]">>|ThingFields],
                                case {lists:member(K, LegalFields), lists:member(K, ThingFields)} of
                                    {true, true} ->
                                        TdF = alinkiot_tdengine_common:to_td_field_name(K),
                                        Acc#{TdF => V};
                                    {true, _} ->
                                        Acc#{K => V};
                                    _ ->
                                        Acc
                                end
                            end, #{}, Args),
                    Args1 =
                        case {maps:get(<<"orderBy[desc]">>, Args, undefined),
                            maps:get(<<"orderBy[asc]">>, Args, undefined)} of
                            {undefined, undefined} ->
                                Args#{<<"fields">> => ThingInfo, <<"where">> => Where};
                            {Desc, _} when Desc =/= undefined ->
                                case lists:member(Desc, [<<"time">>, <<"addr">>, <<"createTime">>]) of
                                    false ->
                                        NDesc = alinkiot_tdengine_common:to_td_field_name(Desc),
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where, <<"orderBy[desc]">> => NDesc};
                                    true ->
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where}
                                end;
                            {_, Asc} ->
                                case lists:member(Asc, [<<"time">>, <<"addr">>, <<"createTime">>]) of
                                    false ->
                                        NAsc = alinkiot_tdengine_common:to_td_field_name(Asc),
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where, <<"orderBy[asc]">> => NAsc};
                                    true ->
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where}
                                end
                        end,
                    case query_alink_by_stable(Args1#{<<"timeAs">> => <<"createTime">>, <<"productId">> => PId}) of
                        {ok, Count, Rows} ->
                            {ok, Count, alinkiot_tdengine_common:rows_from_td_field_name(Rows)};
                        {ok, Rows} ->
                            {ok, alinkiot_tdengine_common:rows_from_td_field_name(Rows)};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {ok, _} ->
                    logger:error("device ~p thing error", [AddrFirst]),
                    {ok, []};
                Err ->
                    {error, Err}
            end;
        Err ->
            {error, Err}
    end.


query_alink_by_stable(#{<<"addrs">> := Addrs} = Param) ->
    Table = parse_table(Param),
    Interval = parse_interval(Param),
    TimeAs = maps:get(<<"timeAs">>, Param, <<"time">>),
    Function = maps:get(<<"function">> , Param, <<>>),
    FieldsBin = to_fields_bin(Interval, maps:get(<<"fields">>, Param, []), TimeAs, Function),
    OrderBy = parse_order_by(Param),
    ParseWhere = parse_where(Param),
    ParseWhere1 =
        case maps:get(<<"addrs">>, Param, []) of
            [] ->
                ParseWhere;
            Addrs ->
                PW0 =
                    lists:foldl(
                        fun(Addr, <<>>) ->
                            <<"'", Addr/binary, "'">>;
                           (Addr, Acc) ->
                            <<Acc/binary, ", '", Addr/binary, "'">>
                    end, <<>>, Addrs),
                <<ParseWhere/binary, " and addr in (", PW0/binary, ")">>
        end,
    GroupBy = parse_group_by(Param),
    Sql =
        case Interval of
            <<>> ->
                <<"select ", FieldsBin/binary, " from ", Table/binary,
                    " where 1 = 1 ", ParseWhere1/binary, GroupBy/binary, OrderBy/binary>>;
            _ ->
                <<"select ", FieldsBin/binary, " from ", Table/binary,
                    " where 1 = 1 ", ParseWhere1/binary, GroupBy/binary, Interval/binary, OrderBy/binary>>
        end,
    case parse_page(Param) of
        <<>> ->
            case foldl_limit_rows(Sql, [], 0) of
                {ok, Rows} ->
                    {ok, Rows};
                {error, Reason} ->
                    {error, Reason}
            end;
        Limit ->
            case request(<<"select count(1) from (", Sql/binary, ")">>) of
                {ok, 0, _} ->
                    {ok, 0, []};
                {ok, _, [#{<<"count(1)">> := Count}]} ->
                    case request(<<Sql/binary, Limit/binary>>) of
                        {ok, _, Rows} ->
                            {ok, Count, Rows};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, #{<<"code">> := 9826,<<"desc">> := <<"Fail to get table info, error: Table does not exist">>}} ->
                    {ok, 0, []};
                {error, Reason} ->
                    {error, Reason}
            end
    end.




do_query_history(#{<<"addr">> := Addr} = Args) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"product">> := PId}} ->
            case alinkcore_cache:query_product(PId) of
                {ok, #{<<"thing">> := Thing}} when Thing =/= []
                                                andalso is_list(Thing)->
                    ExtraFields = maps:get(<<"extra_fields">>, Args, []),
                    ThingInfo =
                        lists:map(
                            fun(#{<<"name">> := F} = FInfo) ->
                                TdF = alinkiot_tdengine_common:to_td_field_name(F),
                                FInfo#{<<"name">> => TdF, <<"as">> => TdF}
                        end, Thing) ++ ExtraFields,
                    ThingFields = lists:map(fun(#{<<"name">> := F}) -> F end, ThingInfo),
                    Where =
                        maps:fold(
                            fun(K, V, Acc) ->
                                LegalFields = [<<"createTime">>, <<"createTime[begin]">>, <<"createTime[end]">>,
                                                <<"time">>, <<"time[begin]">>, <<"time[end]">>|ThingFields],
                                case {lists:member(K, LegalFields), lists:member(K, ThingFields)} of
                                    {true, true} ->
                                        TdF = alinkiot_tdengine_common:to_td_field_name(K),
                                        Acc#{TdF => V};
                                    {true, _} ->
                                        Acc#{K => V};
                                    _ ->
                                        Acc
                                end
                        end, #{}, Args),
                    Args1 =
                        case {maps:get(<<"orderBy[desc]">>, Args, undefined),
                            maps:get(<<"orderBy[asc]">>, Args, undefined)} of
                            {undefined, undefined} ->
                                Args#{<<"fields">> => ThingInfo, <<"where">> => Where};
                            {Desc, _} when Desc =/= undefined ->
                                case lists:member(Desc, [<<"time">>, <<"addr">>, <<"createTime">>]) of
                                    false ->
                                        NDesc = alinkiot_tdengine_common:to_td_field_name(Desc),
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where, <<"orderBy[desc]">> => NDesc};
                                    true ->
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where}
                                end;
                            {_, Asc} ->
                                case lists:member(Asc, [<<"time">>, <<"addr">>, <<"createTime">>]) of
                                    false ->
                                        NAsc = alinkiot_tdengine_common:to_td_field_name(Asc),
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where, <<"orderBy[asc]">> => NAsc};
                                    true ->
                                        Args#{<<"fields">> => ThingInfo, <<"where">> => Where}
                                end
                        end,
                    case query_alink(Args1#{<<"timeAs">> => <<"createTime">>}) of
                        {ok, Count, Rows} ->
                            {ok, Count, alinkiot_tdengine_common:rows_from_td_field_name(Rows)};
                        {ok, Rows} ->
                            {ok, alinkiot_tdengine_common:rows_from_td_field_name(Rows)};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {ok, _} ->
                    logger:error("device ~p thing error", [Addr]),
                    {ok, []};
                Err ->
                    {error, Err}
            end;
        Err ->
            {error, Err}
    end.


query_alink(Param) ->
    Table = parse_table(Param),
    Interval = parse_interval(Param),
    TimeAs = maps:get(<<"timeAs">>, Param, <<"time">>),
    Function = maps:get(<<"function">> , Param, <<>>),
    FieldsBin = to_fields_bin(Interval, maps:get(<<"fields">>, Param, []), TimeAs, Function),
    OrderBy = parse_order_by(Param),
    ParseWhere = parse_where(Param),
    GroupBy = parse_group_by(Param),
    Sql =
        case Interval of
            <<>> ->
                <<"select ", FieldsBin/binary, " from ", Table/binary,
                    " where 1 = 1 ", ParseWhere/binary, GroupBy/binary, OrderBy/binary>>;
            _ ->
                <<"select ", FieldsBin/binary, " from ", Table/binary,
                    " where 1 = 1 ", ParseWhere/binary, GroupBy/binary, Interval/binary, OrderBy/binary>>
        end,
    case parse_page(Param) of
        <<>> ->
            case request(<<Sql/binary, " LIMIT 30000">>) of
                {ok, _, Rows} ->
                    {ok, Rows};
                {error, #{<<"code">> := 9826,<<"desc">> := <<"Fail to get table info, error: Table does not exist">>}} ->
                    {ok, []};
                {error, Reason} ->
                    {error, Reason}
            end;
        Limit ->
            case request(<<"select count(1) from (", Sql/binary, ")">>) of
                {ok, 0, _} ->
                    {ok, 0, []};
                {ok, _, [#{<<"count(1)">> := Count}]} ->
                    case request(<<Sql/binary, Limit/binary>>) of
                        {ok, _, Rows} ->
                            {ok, Count, Rows};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, #{<<"code">> := 9826,<<"desc">> := <<"Fail to get table info, error: Table does not exist">>}} ->
                    {ok, 0, []};
                {error, Reason} ->
                    {error, Reason}
            end
    end.


foldl_limit_rows(Sql, AlreadyRows, N) ->
    Start = N * ?DEFAULT_LIMIT,
    StartB = alinkutil_type:to_binary(Start),
    DEfaultLimitB = alinkutil_type:to_binary(?DEFAULT_LIMIT),
    case request(<<Sql/binary, " LIMIT ", StartB/binary, ",", DEfaultLimitB/binary>>) of
        {ok, Len, Rows} when Len < ?DEFAULT_LIMIT ->
            {ok, lists:flatten(Rows, AlreadyRows)};
        {ok, _, Rows} ->
            foldl_limit_rows(Sql, lists:flatten(Rows, AlreadyRows), N + 1);
        {error, #{<<"code">> := 9826,<<"desc">> := <<"Fail to get table info, error: Table does not exist">>}} ->
            {ok, []};
        {error, Reason} ->
            {error, Reason}
    end.


parse_table(#{<<"table">> := Table}) ->
    Table;
parse_table(#{<<"addr">> := Addr}) ->
    AddrB = binary:replace(alinkutil_type:to_binary(Addr), <<"-">>, <<"_">>, [global]),
    <<"device_", AddrB/binary>>;
parse_table(#{<<"productId">> := ProductId}) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    <<"alinkiot_", ProductIdB/binary>>.


parse_page(#{<<"type">> := <<"chart">>}) ->
    <<>>;
parse_page(#{<<"pageSize">> := RPageSize, <<"pageNum">> := RPageNum}) when RPageSize =/= undefined
                                                                andalso RPageNum =/= undefined ->
    PageSize = alinkutil_type:to_integer(RPageSize),
    PageNum = alinkutil_type:to_integer(RPageNum),
    Offset = PageSize * (PageNum - 1),
    PageSizeB = alinkutil_type:to_binary(PageSize),
    OffsetB = alinkutil_type:to_binary(Offset),
    <<" LIMIT ", OffsetB/binary, ",", PageSizeB/binary>>;
parse_page(_Param) ->
    <<>>.

parse_interval(#{<<"step">> := Step}) ->
    StepB = alinkutil_type:to_binary(Step),
    <<" interval (", StepB/binary, "s) ">>;
parse_interval(_) ->
    <<>>.


parse_order_by(#{<<"orderBy[desc]">> := Field}) when is_binary(Field)->
    <<" order by ", Field/binary, " desc">>;
parse_order_by(#{<<"orderBy[asc]">> := Field}) when is_binary(Field) ->
    <<" order by ", Field/binary, " asc">>;
parse_order_by(_) ->
    <<>>.

parse_where(#{<<"where">> := Where}) ->
    maps:fold(
        fun(_K, V, Acc) when V =:= null
                            orelse V=:= undefined ->
                Acc;
            (K, V, Acc) when K =:= <<"time[begin]">>
                        orelse K =:= <<"createTime[begin]">> ->
            VB = alinkiot_tdengine_common:to_sql_bin(V),
            <<Acc/binary, " and time >= ", VB/binary>>;
           (K, V, Acc) when K =:= <<"time[end]">>
                        orelse K =:= <<"createTime[end]">>->
            VB = alinkiot_tdengine_common:to_sql_bin(V),
            <<Acc/binary, " and time < ", VB/binary>>;
            (K, V, Acc) ->
            KB = alinkutil_type:to_binary(K),
            VB = alinkiot_tdengine_common:to_sql_bin(V),
            <<Acc/binary, " and ", KB/binary, " = ", VB/binary>>
    end, <<>>, Where);
parse_where(_) ->
    <<>>.

parse_group_by(#{<<"groupBy">> := GroupBy}) ->
    <<" group by ", GroupBy/binary>>;
parse_group_by(_) ->
    <<>>.

%% 下面的代码有点乱七八糟，找机会整理 TODO



%% 查询
query_object(TableName, Query) ->
    Keys = format_keys(Query),
    Order = format_order(Query),
    Limit = format_limit(Query),
    Offset = format_offset(Query),
    Where = format_where(Query),
    Interval = format_interval(Query),
    Fill = format_fill(Query),
    From = format_from(Query),
    Group = format_group(Query),
    Get =
        fun(Keys1) ->
            Tail = list_to_binary(join(" ", [TableName, From, Where], true)),
            Sql = <<"SELECT ", Keys1/binary ," FROM ", Tail/binary>>,
            request(Sql)
        end,
    Fun =
        fun(Keys1) ->
            Tail = list_to_binary(join(" ", [TableName, From, Where, Interval, Fill, Order, Limit, Offset, Group], true)),
            Sql = <<"SELECT ", Keys1/binary, " FROM ", Tail/binary>>,
            request(Sql)
        end,
    case Keys of
        <<"count(*)">> ->
            Get(<<"count(*)">>);
        _ ->
            case re:run(Keys, <<"last_row\\([^\\)]+\\)">>, [{capture,all,binary}]) == nomatch of
                false ->
                    Get(Keys);
                true ->
                    List = re:split(Keys, <<",">>),
                    Is = re:run(Keys, <<"AVG\\([^\\)]+\\)|MAX\\([^\\)]+\\)|MIN\\([^\\)]+\\)">>, [{capture,all,binary}]) == nomatch,
                    case Is andalso lists:member(<<"count(*)">>, List) of
                        true ->
                            case Get(<<"count(*)">>) of
                                {error, Reason} ->
                                    {error, Reason};
                                {ok, _, Result} ->
                                    Keys1 = list_to_binary(join(",", lists:delete(<<"count(*)">>, List), true)),
                                    case Fun(Keys1) of
                                        {ok, Row, Records} ->
                                            {ok, Row, maps:merge(Result, Records)};
                                        {error, Reason} ->
                                            {error, Reason}
                                    end
                            end;
                        false ->
                            Fun(Keys)
                    end
            end
    end.



batch(Requests) when is_list(Requests) ->
    Request1 = list_to_binary(join(" ", [format_batch(Request) || Request <- Requests])),
    Sql = <<"INSERT INTO ", Request1/binary, ";">>,
    request(Sql);
batch(Batch) ->
    Values = format_batch(Batch),
    Sql = <<"INSERT INTO ", Values/binary, ";">>,
    request(Sql).


create_user(UserName, Password) ->
    request(<<"CREATE USER ", UserName/binary, " PASS ‘", Password/binary, "’">>).


delete_user(UserName) ->
    request(<<"DROP USER ", UserName/binary>>).

alter_user(UserName, NewPassword) ->
    request(<<"ALTER USER ", UserName/binary, " PASS ‘", NewPassword/binary, "’">>).


join(Sep, L) -> join(Sep, L, false).
join(Sep, L, Trip) -> join(Sep, L, Trip, fun alinkutil_type:to_binary/1).
join(_Sep, [], _, _) -> [];
join(Sep, [<<>> | T], true, F) -> join(Sep, T, true, F);
join(Sep, [H | T], Trip, F) -> [F(H) | join_prepend(Sep, T, Trip, F)].
join_prepend(_Sep, [], _, _) -> [];
join_prepend(Sep, [<<>> | T], true, F) -> join_prepend(Sep, T, true, F);
join_prepend(Sep, [H | T], Trip, F) -> [Sep, F(H) | join_prepend(Sep, T, Trip, F)].


format_value(V) when is_binary(V) -> <<"'", V/binary, "'">>;
format_value(V) -> alinkutil_type:to_binary(V).

format_column(V) -> alinkutil_type:to_binary(V).


format_db(<<>>) -> <<>>;
format_db(DB) -> <<DB/binary, ".">>.

format_using(_, <<>>) -> <<>>;
format_using(DB, Table) ->
    DB1 = format_db(DB),
    <<" using ", DB1/binary, Table/binary>>.

format_tags(<<>>) -> <<>>;
format_tags(Tags) -> <<" TAGS (", Tags/binary, ")">>.

format_batch(#{<<"tableName">> := TableName, <<"fields">> := Fields0, <<"values">> := Values0} = Batch) ->
    Using = maps:get(<<"using">>, Batch, <<>>),
    Fields = list_to_binary(join(",", Fields0, false, fun format_column/1)),
    Values2 = [list_to_binary("(" ++ join(",", Values1, false, fun format_value/1) ++ ")") || Values1 <- Values0],
    Values = list_to_binary(join(" ", Values2)),
    Using1 = format_using(<<>>, Using),
    Tags = maps:get(<<"tags">>, Batch, []),
    TagFields = format_tags(list_to_binary(join(",", Tags, false, fun format_value/1))),
    <<TableName/binary, Using1/binary, TagFields/binary, " (", Fields/binary, ") VALUES ", Values/binary>>;
format_batch(#{  <<"tableName">> := TableName, <<"values">> := Values0} = Batch) ->
    Using = maps:get(<<"using">>, Batch, <<>>),
    Values2 = [list_to_binary("(" ++ join(",", Values1, false, fun format_value/1) ++ ")") || Values1 <- Values0],
    Values = list_to_binary(join(" ", Values2)),
    Using1 = format_using(<<>>, Using),
    Tags = maps:get(<<"tags">>, Batch, []),
    TagFields = format_tags(list_to_binary(join(",", Tags, false, fun format_value/1))),
    <<TableName/binary, Using1/binary, TagFields/binary, " VALUES ", Values/binary>>.



format_order([], Acc) -> Acc;
format_order([<<"-", Field/binary>> | Other], Acc) ->
    format_order(Other, Acc ++ [<<Field/binary, " DESC">>]);
format_order([<<"+", Field/binary>> | Other], Acc) ->
    format_order([Field | Other], Acc);
format_order([Field | Other], Acc) ->
    format_order(Other, Acc ++ [<<Field/binary, " ASC">>]).
format_order(#{<<"order">> := Order}) when Order =/= <<>>, Order =/= undefined ->
    Order1 = list_to_binary(join(",", format_order(re:split(Order, <<",">>), []))),
    <<"ORDER BY ", Order1/binary>>;
format_order(_) ->
    <<>>.

format_limit(#{<<"limit">> := Limit}) when is_integer(Limit), Limit =/= undefined ->
    L = integer_to_binary(Limit),
    <<"LIMIT ", L/binary>>;
format_limit(_) ->
    <<"LIMIT 5000">>.

format_offset(#{<<"skip">> := Skip}) when Skip =/= undefined, is_integer(Skip) ->
    S = integer_to_binary(Skip),
    <<"OFFSET ", S/binary>>;
format_offset(_) ->
    <<>>.


format_keys(#{<<"keys">> := Keys}) when Keys =/= undefined, Keys =/= <<>> ->
    Keys;
format_keys(_) ->
    <<"*">>.

format_interval(#{<<"interval">> := Interval}) when Interval =/= undefined, is_integer(Interval) ->
    I = integer_to_binary(Interval),
    <<"INTERVAL ", I/binary>>;
format_interval(_) ->
    <<>>.

format_fill(#{<<"fill">> := Value}) when Value =/= <<>>, Value =/= undefined ->
    <<"FILL ", Value/binary>>;
format_fill(_) ->
    <<>>.

format_from(#{<<"from">> := From}) when From =/= <<>>, From =/= undefined ->
    From;
format_from(_) ->
    <<>>.

format_group(#{<<"group">> := Group}) when Group =/= <<>>, Group =/= undefined ->
    <<"GROUP BY ", Group/binary>>;
format_group(_) ->
    <<>>.

format_where([], Acc) ->
    Acc;
format_where([{Field, #{<<"$gt">> := Value}} | Other], Acc) ->
    V = alinkutil_type:to_binary(Value),
    format_where(Other, [<<Field/binary, " > ", V/binary>> | Acc]);
format_where([{Field, #{<<"$gte">> := Value}} | Other], Acc) ->
    V = alinkutil_type:to_binary(Value),
    format_where(Other, [<<Field/binary, " >= ", V/binary>> | Acc]);
format_where([{Field, #{<<"$lt">> := Value}} | Other], Acc) ->
    V = alinkutil_type:to_binary(Value),
    format_where(Other, [<<Field/binary, " < ", V/binary>> | Acc]);
format_where([{Field, #{<<"$gte">> := Value}} | Other], Acc) ->
    V = alinkutil_type:to_binary(Value),
    format_where(Other, [<<Field/binary, " <= ", V/binary>> | Acc]);
format_where([{Field, #{<<"$ne">> := Value}} | Other], Acc) ->
    V = alinkutil_type:to_binary(Value),
    format_where(Other, [<<Field/binary, " <> ", V/binary>> | Acc]);
format_where([{Field, #{<<"$regex">> := Value}} | Other], Acc) ->
    V = alinkutil_type:to_binary(Value),
    format_where(Other, [<<Field/binary, " LIKE '", V/binary, "'">> | Acc]);
format_where([{Field, Value} | Other], Acc) ->
    V = alinkutil_type:to_binary(Value),
    format_where(Other, [<<Field/binary, " = '", V/binary, "'">> | Acc]).
format_where(#{<<"where">> := Where}) when Where =/= undefined, Where =/= <<>> ->
    Where1 =
        case is_list(Where) of
            true ->
                format_where(Where, []);
            false when is_map(Where) ->
                format_where(maps:to_list(Where), [])
        end,
    case list_to_binary(lists:join(" AND ", lists:reverse(Where1))) of
        <<>> ->
            <<>>;
        Where2 ->
            <<"WHERE ", Where2/binary>>
    end;
format_where(_) ->
    <<>>.



request(Sql) ->
    Host = application:get_env(alinkiot_tdengine, host, "127.0.0.1"),
    Port = application:get_env(alinkiot_tdengine, port, 6041),
    Username = application:get_env(alinkiot_tdengine, username, "root"),
    Password = application:get_env(alinkiot_tdengine, password, "taosdata"),
    Database = application:get_env(alinkiot_tdengine, database, ""),
    case alinkiot_tdengine_rest:simple_query(Host, Port, Username, Password, Database, Sql) of
        {ok, Row, Result} ->
            {ok, Row, format_result(Result)};
        {error, Reason} ->
            logger:error("Execute Fail ~p ~p ~p ~p", [Host, Port, Sql, Reason]),
            {error, Reason}
    end.



format_result(#{<<"count(*)">> := Count} = R) -> R#{<<"count">> => Count};
format_result(R) -> R.



%%%===================================================================
%%% Internal functions
%%%===================================================================
create_subtable_fields_bin(Fields) ->
    lists:foldl(
        fun(#{<<"name">> := K, <<"value">> := V}, {Acc0, Acc1}) ->
            KB = alinkutil_type:to_binary(K),
            VB = alinkiot_tdengine_common:to_sql_bin(V),
            case Acc0 of
                <<>> ->
                    {KB, VB};
                _ ->
                    {<<Acc0/binary, ",", KB/binary>>, <<Acc1/binary, ",", VB/binary>>}
            end
        end, {<<>>, <<>>}, Fields).

create_table_fields_bin(Fields) ->
    lists:foldl(
        fun(#{<<"name">> := Name, <<"type">> := Type}, Acc) ->
            RealType = alinkiot_tdengine_common:transform_field_type(Type),
            <<Acc/binary, ",", Name/binary, " ", RealType/binary>>
        end, <<>>, Fields).


create_table_tags_bin(Tags) ->
    lists:foldl(
        fun(#{<<"name">> := Name, <<"type">> := Type}, Acc) ->
            RealType = alinkiot_tdengine_common:transform_field_type(Type),
            case Acc of
                <<>> ->
                    <<Name/binary, " ", RealType/binary>>;
                _ ->
                    <<Acc/binary, ",", Name/binary, " ", RealType/binary>>
            end
    end, <<>>, Tags).

to_fields_bin(<<>>, [], <<>>, _Function) ->
    <<"*">>;
to_fields_bin(<<>>, Fields, TimeAs, <<>>) ->
    Fields1 = lists:filter(fun(#{<<"name">> := F}) -> F =/= <<"time">> end, Fields),
    TimeB = <<"TIMEDIFF(time, 0, 1s) as ", TimeAs/binary>>,
    lists:foldl(
        fun(#{<<"name">> := F} = FInfo, Acc) ->
            Alias = maps:get(<<"as">>, FInfo, F),
            <<Acc/binary, ",", F/binary, " as ", Alias/binary>>
    end, TimeB, Fields1);
to_fields_bin(<<>>, Fields, TimeAs, Function) ->
    Fields1 = lists:filter(fun(#{<<"name">> := F}) -> F =/= <<"time">> end, Fields),
    TimeB = <<"TIMEDIFF(", Function/binary, "(time), 0, 1s) as ", TimeAs/binary>>,
    lists:foldl(
        fun(#{<<"name">> := F} = FInfo, Acc) ->
            Alias = maps:get(<<"as">>, FInfo, F),
            <<Acc/binary, ",", Function/binary, "(" ,F/binary, ") as ", Alias/binary>>
        end, TimeB, Fields1);
to_fields_bin(_, Fields, TimeAs, _Function) ->
    Fields1 = lists:filter(fun(#{<<"name">> := F}) -> F =/= <<"time">> end, Fields),
    lists:foldl(
        fun(#{<<"name">> := F,
              <<"type">> := Type} = FInfo, Acc) when Type =:= <<"int">>
                                                orelse Type =:= <<"integer">>
                                                orelse Type =:= <<"float">> ->
            Alias = maps:get(<<"as">>, FInfo, F),
            <<Acc/binary, ", avg(", F/binary, ") as ", Alias/binary>>;
           (#{<<"name">> := F,
            <<"type">> := Type} = FInfo, Acc) when Type =:= <<"string">> orelse Type =:= <<"array">> ->
            Alias = maps:get(<<"as">>, FInfo, F),
            <<Acc/binary, ", last(", F/binary, ") as ", Alias/binary>>
        end, <<"TIMEDIFF(_wstart, 0, 1s) as ", TimeAs/binary>>, Fields1).


return_format_time(#{<<"timeFormat">> := <<"datetime">>}, Rows) ->
    lists:map(
        fun(#{<<"createtime">> := Time} = Row) ->
            Row#{<<"createtime">> => timestamp2localtime_str(Time)};
            (#{<<"time">> := Time} = Row) ->
                Row#{<<"time">> => timestamp2localtime_str(Time)};
            (Row) ->
                Row
    end, Rows);
return_format_time(_, Rows) ->
    Rows.


return_format_time_row(#{<<"timeFormat">> := <<"datetime">>}, #{<<"createtime">> := Time} = Row) ->
    Row#{<<"createtime">> => timestamp2localtime_str(Time)};
return_format_time_row(#{<<"timeFormat">> := <<"datetime">>}, #{<<"time">> := Time} = Row) ->
    Row#{<<"time">> => timestamp2localtime_str(Time)};
return_format_time_row(_, Row) ->
    Row.


timestamp2localtime_str(TimeStamp) ->
    {{Y, M, D}, {H, Mi, S}} = calendar:gregorian_seconds_to_datetime(TimeStamp + 3600 *8 + calendar:datetime_to_gregorian_seconds({{1970,1,1}, {0,0,0}})),
    list_to_binary(lists:flatten(io_lib:format("~w-~2..0w-~2..0w ~2..0w:~2..0w:~2..0w",[Y,M,D,H,Mi,S]))).