%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 07. 8月 2022 下午5:05
%%%-------------------------------------------------------------------
-module(alinkdata_dao).
-author("yqfclid").

%% API
-export([
    query/2,
    query_user_by_user_name/1,
    query_user_by_open_id/1,
    query_no_count/2,
    query/3,
    build_sql_with_dao_id/3,
    build_sql/3,
    transform_k/1,
    query_user_by_user_id/1,
    query_user_by_phone_number/1,
    query_user_by_mini_union_id/1,
    query_user_by_mini_id/1
]).

-include("alinkdata.hrl").
%%%===================================================================
%%% API
%%%===================================================================
query(Dao, RawArgs) ->
    query(Dao, RawArgs, true).

%对sys_user特殊处理
query_user_by_user_name(RawArgs) ->
    case query(select_user_by_user_name, RawArgs, false) of
        {ok, ReturnData} ->
            {ok, build_user_info(ReturnData)};
        Other ->
            Other
    end.


%对sys_user特殊处理
query_user_by_open_id(OpenId) ->
    case alinkdata_dao:query_no_count('QUERY_wechat', #{<<"openId">> => OpenId}) of
        {ok, [#{<<"id">> := WechatId}]} ->
            query_user_by_wechat_id(#{<<"wechat">> => WechatId});
        {ok, []} ->
            {ok, []};
        Other ->
            Other
    end.


%对sys_user特殊处理
query_user_by_user_id(RawArgs) ->
    case query(select_user_by_id, RawArgs, false) of
        {ok, ReturnData} ->
            {ok, build_user_info(ReturnData)};
        Other ->
            Other
    end.


query_user_by_phone_number(RawArgs) ->
    case query(select_user_by_phone, RawArgs, false) of
        {ok, ReturnData} ->
            {ok, build_user_info(ReturnData)};
        Other ->
            Other
    end.

query_user_by_mini_union_id(RawArgs) ->
    case alinkdata_dao:query_no_count('QUERY_wechat_mini', #{<<"unionId">> => RawArgs}) of
        {ok, [#{<<"id">> := MiniId}]} ->
            query_user_by_mini_id(#{<<"miniId">> => MiniId});
        {ok, []} ->
            {ok, []};
        Other ->
            Other
    end.


query_user_by_mini_id(RawArgs) ->
    case query(select_user_by_mini, RawArgs, false) of
        {ok, ReturnData} ->
            {ok, build_user_info(ReturnData)};
        Other ->
            Other
    end.


%对sys_user特殊处理
query_user_by_wechat_id(RawArgs) ->
    case query(select_user_by_wechat, RawArgs, false) of
        {ok, ReturnData} ->
            {ok, build_user_info(ReturnData)};
        Other ->
            Other
    end.


query_no_count(Dao, RawArgs) ->
    query(Dao, RawArgs, false).

query(DaoId, RawArgs, NeedCount) ->
    Args = transform_time_fields(RawArgs),
    case alinkdata_dao_cache:get_dao(DaoId) of
        {ok, Dao} ->
            Sql = build_sql(Dao, Args, NeedCount),
            case alinkdata:query_mysql_format_map(default, Sql) of
                ok ->
                    ok;
                {ok, [[#{<<"count">> := Count}], Datas]} ->
                    ReturnDatas = lists:map(fun transform_field/1, Datas),
                    {ok, Count, ReturnDatas};
                {ok, Datas} ->
                    ReturnDatas = lists:map(fun transform_field/1, Datas),
                    {ok, ReturnDatas};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

build_sql_with_dao_id(DaoId, Args, NeedCount) ->
    case alinkdata_dao_cache:get_dao(DaoId) of
        {ok, Dao} ->
            {ok, build_sql(Dao, Args, NeedCount)};
        {error, Reason} ->
            {error, Reason}
    end.

build_sql(Dao, Args, false) ->
    build_dao_sql(Dao, Args, <<>>);
build_sql(Dao, Args, true) ->
    case maps:get(<<"pageSize">>, Args, undefined) of
        undefined ->
            build_dao_sql(Dao, Args, <<>>);
        _ ->
            DaoSql = build_dao_sql(Dao, Args, <<>>),
            PageSql = build_page_sql(Args),
            Paged = <<DaoSql/binary, PageSql/binary>>,
            Countd = <<"select count(1) as count from (", DaoSql/binary, ") a;">>,
            <<Countd/binary, Paged/binary>>
    end.


%%%===================================================================
%%% Internal functions
%%%===================================================================
build_user_info([]) ->
    [];
build_user_info([R|_] = Ret) ->
    UserKeys = [
        <<"userId">>,
        <<"deptId">>,
        <<"userName">>,
        <<"nickName">>,
        <<"email">>,
        <<"phonenumber">>,
        <<"sex">>,
        <<"avatar">>,
        <<"password">>,
        <<"status">>,
        <<"delFlag">>,
        <<"loginIp">>,
        <<"loginDate">>,
        <<"createBy">>,
        <<"createTime">>,
        <<"updateBy">>,
        <<"updateTime">>,
        <<"remark">>,
        <<"wechat">>,
        <<"mini">>
    ],
    DeptKeys = [
        <<"parentId">>,
        <<"deptName">>,
        <<"orderNum">>,
        <<"leader">>,
        <<"deptStatus">>
    ],
    RoleKeys = [
        <<"roleId">>,
        <<"roleName">> ,
        <<"roleKey">> ,
        <<"roleSort">>,
        <<"dataScope">>,
        <<"roleStatus">>
    ],
    {User, Dept} =
        maps:fold(
            fun(K, V, {U, D}) ->
                NU =
                    case lists:member(K, UserKeys) of
                        true ->
                            U#{K => V};
                        false ->
                            U
                    end,
                ND =
                    case lists:member(K, DeptKeys) of
                        true ->
                            D#{K => V};
                        false ->
                            D
                    end,
                {NU, ND}
        end, {#{}, #{}}, R),
    Roles =
        lists:map(
            fun(RetDetail) ->
                maps:fold(
                    fun(K, V, Acc) ->
                        case lists:member(K, RoleKeys) of
                            true ->
                                Acc#{K => V};
                            false ->
                                Acc
                        end
                end, #{}, RetDetail)
        end, Ret),

    [User#{<<"dept">> => Dept, <<"roles">> => Roles}].


transform_field(Data) ->
    maps:fold(
        fun(K, V, Acc) ->
            NK = transform_k(K),
            NV = transform_time_v(V),
            Acc#{NK => NV}
    end, #{}, Data).

transform_k(K) ->
    case binary:split(K, <<"_">>, [global]) of
        [Field] ->
            Field;
        [Field1 | Fields] ->
            UpperFirst =
                fun(<<A:1/bytes, L/binary>>) ->
                    A1 = string:uppercase(A),
                    binary_to_list(<<A1/binary, L/binary>>)
                end,
            list_to_binary(lists:concat([binary_to_list(Field1) | [UpperFirst(F) || F <- Fields]]))
    end.

transform_time_fields(Args) ->
    maps:fold(
        fun(K, V, Acc) ->
            Acc#{transform_time_k(K) => transform_time_v(V)}
    end, #{}, Args).

transform_time_k(<<"params[beginTime]">>) -> <<"beginTime">>;
transform_time_k(<<"params[endTime]">>) -> <<"endTime">>;
transform_time_k(K) -> K.

transform_time_v({{Y,M,D},{H,N,S}}) ->
    list_to_binary(lists:concat([Y, "-", M, "-", D, " ", H, ":", N, ":", S]));
transform_time_v(Time) ->
    Time.





build_page_sql(Args) ->
    PageSize = maps:get(<<"pageSize">>, Args, undefined),
    PageNum = maps:get(<<"pageNum">>, Args, undefined),
    do_build_page_sql(PageNum, PageSize).

do_build_page_sql(_, undefined) ->
    <<>>;
do_build_page_sql(undefined, PageSize) ->
    list_to_binary(lists:concat([" LIMIT ", PageSize]));
do_build_page_sql(PageNum, PageSize) ->
    list_to_binary(lists:concat([" LIMIT ", (PageNum - 1) * PageSize, ",", PageSize])).






build_dao_sql([], _Args, Return) ->
    Return;
build_dao_sql([{key, Key}|T], Args, Return) ->
    Sql = to_binary(maps:get(Key, Args, <<>>)),
    NReturn = <<Return/binary, Sql/binary>>,
    build_dao_sql(T, Args, NReturn);
build_dao_sql([{key, Type, Key}|T], Args, Return) ->
    Sql = to_binary(Type, maps:get(Key, Args, <<>>)),
    NReturn = <<Return/binary, Sql/binary>>,
    build_dao_sql(T, Args, NReturn);
build_dao_sql([{dao, DaoId}|T], Args, Return) ->
    {ok, Dao} = alinkdata_dao_cache:get_dao(DaoId),
    Sql = build_dao_sql(Dao, Args, <<>>),
    build_dao_sql(T, Args, <<Return/binary, Sql/binary>>);
build_dao_sql([{foreach_spilt, Sep, {key, Type, Key}}|T], Args, Return) ->
    RValue = maps:get(Key, Args, <<>>),
    Value = binary:split(RValue, <<",">>, [global]),
    Sql =
        lists:foldl(
            fun(SubV, <<>>) ->
                to_binary(Type, SubV);
                (SubV, Acc) ->
                    SubVB = to_binary(Type, SubV),
                    <<Acc/binary, Sep/binary, SubVB/binary>>
            end, <<>>, Value),
    build_dao_sql(T, Args, <<Return/binary, Sql/binary>>);
build_dao_sql([{foreach, Sep, {key, Type, Key}}|T], Args, Return) ->
    Value = maps:get(Key, Args, []),
    Sql =
        lists:foldl(
            fun(SubV, <<>>) ->
                to_binary(Type, SubV);
               (SubV, Acc) ->
                   SubVB = to_binary(Type, SubV),
                <<Acc/binary, Sep/binary, SubVB/binary>>
        end, <<>>, Value),
    build_dao_sql(T, Args, <<Return/binary, Sql/binary>>);
build_dao_sql([{list_map, Sep, MapKey, KeyList}|T], Args, Return) ->
    Value = maps:get(MapKey, Args, []),
    Sql =
        lists:foldl(
            fun(SubV, Acc) ->
                SubSql =
                    lists:foldl(
                        fun({key, Type, K}, Acc2) ->
                            V = maps:get(K, SubV),
                            S = to_binary(Type, V),
                            <<Acc2/binary, S/binary>>;
                           (S, Acc2) ->
                            <<Acc2/binary, S/binary>>
                    end, <<>>, KeyList),
                case Acc of
                    <<>> ->
                        SubSql;
                    _ ->
                        <<Acc/binary, Sep/binary, SubSql/binary>>
                end
            end, <<>>, Value),
    build_dao_sql(T, Args, <<Return/binary, Sql/binary>>);
build_dao_sql([Sql|T], Args, Return) when is_binary(Sql) ->
    NReturn = <<Return/binary, Sql/binary>>,
    build_dao_sql(T, Args, NReturn);
build_dao_sql([remove_tailing_comma|T], Args, <<>>) ->
    build_dao_sql(T, Args, <<>>);
build_dao_sql([remove_tailing_comma|T], Args, Return) ->
    NReturn =
    case binary:last(Return) of
        $, ->
            binary:part(Return, {0, byte_size(Return) - 1});
        _ ->
            Return
    end,
    build_dao_sql(T, Args, NReturn);
build_dao_sql([{require, Require, SqlPattern}|T], Args, Return) ->
    case check_require(Require, Args) of
        true ->
            Sql = build_dao_pattern(SqlPattern, Args),
            build_dao_sql(T, Args, <<Return/binary, Sql/binary>>);
        false ->
            build_dao_sql(T, Args, Return)
    end.


check_require({notin, Key, IlegalValues}, Args) ->
    not lists:member(maps:get(Key, Args, undefined), IlegalValues);
check_require({in, Key, LegalValues}, Args) ->
    lists:member(maps:get(Key, Args, undefined), LegalValues);
check_require(Requires, Args) when is_list(Requires)->
    lists:all(
        fun(Require) ->
            check_require(Require, Args)
    end, Requires).


build_dao_pattern(SqlPattern, Args) ->
    do_build_dao_pattern(SqlPattern, Args, <<>>).

do_build_dao_pattern([], _Args, Return) ->
    Return;
do_build_dao_pattern([{dao, DaoId}|T], Args, Return) ->
    {ok, Dao} = alinkdata_dao_cache:get_dao(DaoId),
    Sql = build_dao_sql(Dao, Args, <<>>),
    do_build_dao_pattern(T, Args, <<Return/binary, Sql/binary>>);
do_build_dao_pattern([{key, Key}|T], Args, Return) ->
    SubSql = to_binary(maps:get(Key, Args, <<>>)),
    do_build_dao_pattern(T, Args, <<Return/binary, SubSql/binary>>);
do_build_dao_pattern([{key, Type, Key}|T], Args, Return) ->
    SubSql = to_binary(Type, maps:get(Key, Args, <<>>)),
    do_build_dao_pattern(T, Args, <<Return/binary, SubSql/binary>>);
do_build_dao_pattern([{foreach_spilt, Sep, {key, Type, Key}}|T], Args, Return) ->
    RValue = maps:get(Key, Args),
    Value = binary:split(RValue, <<",">>, [global]),
    SubSql =
        lists:foldl(
            fun(SubV, <<>>) ->
                to_binary(Type, SubV);
                (SubV, Acc) ->
                    SubVB = to_binary(Type, SubV),
                    <<Acc/binary, Sep/binary, SubVB/binary>>
            end, <<>>, Value),
    do_build_dao_pattern(T, Args, <<Return/binary, SubSql/binary>>);
do_build_dao_pattern([{foreach, Sep, {key, Type, Key}}|T], Args, Return) ->
    Value = maps:get(Key, Args),
    SubSql =
        lists:foldl(
            fun(SubV, <<>>) ->
                to_binary(Type, SubV);
                (SubV, Acc) ->
                    SubVB = to_binary(Type, SubV),
                    <<Acc/binary, Sep/binary, SubVB/binary>>
            end, <<>>, Value),
    do_build_dao_pattern(T, Args, <<Return/binary, SubSql/binary>>);
do_build_dao_pattern([{list_map, Sep, MapKey, KeyList}|T], Args, Return) ->
    Value = maps:get(MapKey, Args, []),
    Sql =
        lists:foldl(
            fun(SubV, Acc) ->
                SubSql =
                    lists:foldl(
                        fun({key, Type, K}, Acc2) ->
                            V = maps:get(K, SubV),
                            S = to_binary(Type, V),
                            <<Acc2/binary, S/binary>>;
                            (S, Acc2) ->
                                <<Acc2/binary, S/binary>>
                        end, <<>>, KeyList),
                case Acc of
                    <<>> ->
                        SubSql;
                    _ ->
                        <<Acc/binary, Sep/binary, SubSql/binary>>
                end
            end, <<>>, Value),
    build_dao_sql(T, Args, <<Return/binary, Sql/binary>>);
do_build_dao_pattern([SubSql|T], Args, Return) when is_binary(SubSql) ->
    do_build_dao_pattern(T,Args, <<Return/binary, SubSql/binary>>).



to_binary(S) when is_integer(S) ->
    integer_to_binary(S);
to_binary(S) when is_binary(S) ->
    <<"'", S/binary, "'">>.


to_binary(integer, true) ->
    <<"1">>;
to_binary(integer, false) ->
    <<"0">>;
to_binary(integer, S) when is_integer(S) ->
    integer_to_binary(S);
to_binary(integer, S) when is_binary(S) ->
    S;
to_binary(float, S) when is_float(S) ->
    float_to_binary(S);
to_binary(float, S) when is_binary(S) ->
    S;
to_binary(str, S) when is_binary(S) ->
    <<"'", S/binary, "'">>;
to_binary(sql, S) when is_binary(S) ->
    S;
to_binary({map_value, Map}, S) ->
    maps:get(S, Map).