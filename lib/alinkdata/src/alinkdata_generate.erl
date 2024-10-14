%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 21. 2月 2023 下午10:42
%%%-------------------------------------------------------------------
-module(alinkdata_generate).
-author("yqfclid").

-include("alinkdata.hrl").

%% API
-export([
    generate_swagger_table_infos/0,
    generate_tables_daos/0
]).

%%%===================================================================
%%% API
%%%===================================================================
generate_tables_daos() ->
    {ok, ConnPid} = mysql:start_link(hd(application:get_env(alinkdata, mysql, []))),
    {ok, _, RawTables} = mysql:query(ConnPid, <<"show tables">>),
    Tables = lists:filter(fun(T) -> not lists:member(T, system_table()) end, lists:flatten(RawTables)),
    Daos = build_tables_daos(ConnPid, Tables),
    mysql:stop(ConnPid),
    Daos.


generate_swagger_table_infos() ->
    {ok, Pid} = mysql:start_link(hd(application:get_env(alinkdata, mysql, []))),
    {ok, _, RawTables} = mysql:query(Pid, <<"show tables">>),
    Tables = lists:filter(fun(T) -> not lists:member(T, system_table()) end, lists:flatten(RawTables)),
    TableInfos = lists:map(fun(Table) -> table_info(Pid, Table) end, Tables),
    mysql:stop(Pid),
    TableInfos.
%%%===================================================================
%%% Internal functions
%%%===================================================================
build_tables_daos(ConnPid, Tables) ->
    do_build_tables_daos(ConnPid, Tables, []).



do_build_tables_daos(_ConnPid, [], Daos) ->
    Daos;
do_build_tables_daos(ConnPid, [Table|TailTables], Daos) ->
    {ok, Fields} = alinkdata:format_result_to_map(mysql:query(ConnPid, <<"describe ", Table/binary>>)),
    PutDaos = put_daos(Table, Fields),
    PostDaos = post_daos(Table, Fields),
    GetDaos = get_daos_more(Table, Fields),
    DeleteDaos = delete_daos(Table, Fields),
    ListDaos = list_daos(Table, Fields),
    LeftJoinDaos = left_join_daos(Table, Fields),
    NDaos = [PutDaos, PostDaos, GetDaos, DeleteDaos, ListDaos, LeftJoinDaos|Daos],
    do_build_tables_daos(ConnPid, TailTables, NDaos).



post_daos(Table, FiledsInfo) ->
    DaoInfoHead = [<<"insert into ", Table/binary, " (">>],
    FiledsInfoLen = length(FiledsInfo),
    DaoInfoDetail0 =
        lists:foldl(
            fun(#{
                <<"Default">> := _Default,
                <<"Extra">> := _Extra,
                <<"Field">> := FieldName,
                <<"Key">> := _Key,
                <<"Null">> := _IsNull,
                <<"Type">> := _Type
            }, Acc) ->
                FieldArg = alinkdata_dao:transform_k(FieldName),
                Content =
                    case {(FiledsInfoLen - 1) =< length(Acc), FieldName =:= <<"update_time">>}  of
                        {true, false} ->
                            {
                                require,
                                [{notin, FieldArg, [null, undefined]}],
                                [<<FieldName/binary>>]
                            };
                        {false, false} ->
                            {
                                require,
                                [{notin, FieldArg, [null, undefined]}],
                                [<<FieldName/binary, ",">>]
                            };
                        {false, true} ->
                            [<<"update_time">>];
                        {true, true} ->
                            [<<"update_time,">>]
                    end,
                Acc ++ [Content]
            end, [], FiledsInfo),


    DaoInfoDetail1 = [<<") values(">>],

    DaoInfoDetail2 =
        lists:foldl(
            fun(#{
                <<"Default">> := _Default,
                <<"Extra">> := _Extra,
                <<"Field">> := FieldName,
                <<"Key">> := _Key,
                <<"Null">> := _IsNull,
                <<"Type">> := Type
            }, Acc) ->
                FieldArg = alinkdata_dao:transform_k(FieldName),
                Content =
                    case {(FiledsInfoLen - 1) =< length(Acc), FieldName =:= <<"update_time">>}  of
                        {true, false} ->
                            {
                                require,
                                [{notin, FieldArg, [null, undefined]}],
                                [{key, get_key_type(Type), FieldArg}]
                            };
                        {false, false} ->
                            {
                                require,
                                [{notin, FieldArg, [null, undefined]}],
                                [
                                    {key, get_key_type(Type), FieldArg},
                                    <<",">>
                                ]
                            };
                        {false, true} ->
                            [<<"sysdate(),">>];
                        {true, true} ->
                            [<<"sysdate()">>]
                    end,
                Acc ++ [Content]
            end, [], FiledsInfo),
    DaoInfoTail = [<<")">>],
    DaoInfo = DaoInfoHead ++ DaoInfoDetail0 ++ [remove_tailing_comma] ++ DaoInfoDetail1 ++ DaoInfoDetail2 ++ [remove_tailing_comma] ++ DaoInfoTail,
    APiTable = get_api_table(Table),
    {binary_to_atom(<<"POST_", APiTable/binary>>, latin1), DaoInfo, APiTable}.



get_daos(Table, FiledsInfo) ->
    DaoInfoHead = [<<"select * from ", Table/binary, " where 1 = 1 ">>],
    DaoInfoDetail0 =
        lists:foldl(
            fun(#{
                <<"Default">> := _Default,
                <<"Extra">> := _Extra,
                <<"Field">> := FieldName,
                <<"Key">> := _Key,
                <<"Null">> := _IsNull,
                <<"Type">> := Type
            }, Acc) ->
                FieldArg = alinkdata_dao:transform_k(FieldName),
                Content =
                    {
                        require,
                        [{notin, FieldArg, [null, undefined, 0]}],
                        [
                            <<" AND ", FieldName/binary, " = ">>,
                            {key, get_key_type((Type)), FieldArg}
                        ]
                    },
                Acc ++ [Content]
            end, [], FiledsInfo),

    [PriMaryFieldInfo] =
        lists:filter(
            fun(#{<<"Key">> := Key}) ->
                Key =:= <<"PRI">>
            end, FiledsInfo),
    #{<<"Field">> := PriFieldName} = PriMaryFieldInfo,

    DaoInfoTail = [
        <<" order by ", PriFieldName/binary>>
    ],
    DaoInfo = DaoInfoHead ++ DaoInfoDetail0 ++ DaoInfoTail,
    APiTable = get_api_table(Table),
    {binary_to_atom(<<"GET_", APiTable/binary>>, latin1), DaoInfo, APiTable}.

get_daos_more(Table, FiledsInfo) ->
    [PriMaryFieldInfo] =
        lists:filter(
            fun(#{<<"Key">> := Key}) ->
                Key =:= <<"PRI">>
            end, FiledsInfo),
    #{<<"Field">> := FieldName, <<"Type">> := Type} = PriMaryFieldInfo,
    FieldArg = alinkdata_dao:transform_k(FieldName),
    DaoInfo = [
        <<"select * from ", Table/binary, " where ", FieldName/binary, " in ( ">>,
        {foreach, <<",">>, {key, get_key_type(Type), FieldArg}},
        <<")">>
    ],
    APiTable = get_api_table(Table),
    {binary_to_atom(<<"GET_", APiTable/binary>>, latin1), DaoInfo, APiTable}.


put_daos(Table, FiledsInfo) ->
    DaoInfoHead = [<<"update ", Table/binary, " set ">>],
    NoPrimaryFieldsInfo =
        lists:filter(
            fun(#{<<"Key">> := Key}) ->
                Key =/= <<"PRI">>
            end, FiledsInfo),
    NoPrimaryFiledsInfoLen = length(NoPrimaryFieldsInfo),
    DaoInfoDetail0 =
        lists:foldl(
            fun(#{
                <<"Default">> := _Default,
                <<"Extra">> := _Extra,
                <<"Field">> := FieldName,
                <<"Key">> := _Key,
                <<"Null">> := _IsNull,
                <<"Type">> := Type
            }, Acc) ->
                FieldArg = alinkdata_dao:transform_k(FieldName),
                Content =
                    case {(NoPrimaryFiledsInfoLen - 1) =< length(Acc), FieldName =:= <<"update_time">>}  of
                        {true, false} ->
                            {
                                require,
                                [{notin, FieldArg, [undefined]}],
                                [
                                    <<FieldName/binary, "= ">>,
                                    {key, get_key_type(Type), FieldArg}
                                ]
                            };
                        {false, false} ->
                            {
                                require,
                                [{notin, FieldArg, [undefined]}],
                                [
                                    <<FieldName/binary, "= ">>,
                                    {key, get_key_type(Type), FieldArg},
                                    <<",">>
                                ]
                            };
                        {false, true} ->
                            [<<"update_time = sysdate() ">>];
                        {true, true} ->
                           [<<"update_time = sysdate(), ">>]
                    end,
                Acc ++ [Content]
            end, [], NoPrimaryFieldsInfo),

    [PriMaryFieldInfo] =
        lists:filter(
            fun(#{<<"Key">> := Key}) ->
                Key =:= <<"PRI">>
            end, FiledsInfo),
    #{<<"Field">> := PriFieldName, <<"Type">> := PrimaryType} = PriMaryFieldInfo,

    DaoInfoDetail1 = [
        <<" where ", PriFieldName/binary, " = ">>,
        {key, get_key_type(PrimaryType), alinkdata_dao:transform_k(PriFieldName)}
    ],
    DaoInfo = DaoInfoHead ++ DaoInfoDetail0 ++ [remove_tailing_comma] ++ DaoInfoDetail1,
    APiTable = get_api_table(Table),
    {binary_to_atom(<<"PUT_", APiTable/binary>>, latin1), DaoInfo, APiTable}.




delete_daos(Table, FiledsInfo) ->
    [PriMaryFieldInfo] =
        lists:filter(
            fun(#{<<"Key">> := Key}) ->
                Key =:= <<"PRI">>
        end, FiledsInfo),
    #{<<"Field">> := FieldName, <<"Type">> := Type} = PriMaryFieldInfo,
    FieldArg = alinkdata_dao:transform_k(FieldName),
    DaoInfo = [
        <<"delete from ", Table/binary, " where ", FieldName/binary, " in ( ">>,
        {foreach, <<",">>, {key, get_key_type(Type), FieldArg}},
        <<")">>
    ],
    APiTable = get_api_table(Table),
    {binary_to_atom(<<"DELETE_", APiTable/binary>>, latin1), DaoInfo, APiTable}.

list_daos(<<"sys_device">>, FiledsInfo) ->
    SelectT2 =
        {
            require,
            [{notin, <<"nodeType">>, [null, undefined, <<>>]}],
            [
                <<",t2.protocol,t2.node_type">>
            ]
        },
    T2 =
        {
            require,
            [{notin, <<"nodeType">>, [null, undefined, <<>>]}],
            [
                <<", sys_product t2">>
            ]
        },
    DaoInfoHead = [<<"select t1.*">>, SelectT2, <<" from sys_device t1">>, T2, <<" where 1 = 1 ">>],
    DaoInfoDetail0 =
        lists:foldl(
            fun(#{
                <<"Default">> := _Default,
                <<"Extra">> := _Extra,
                <<"Field">> := FieldName,
                <<"Key">> := _Key,
                <<"Null">> := _IsNull,
                <<"Type">> := Type
            }, Acc) ->
                FieldArg = alinkdata_dao:transform_k(FieldName),
                Content =
                    {
                        require,
                        [{notin, FieldArg, [null, undefined, 0]}],
                        [
                            <<" AND t1.", FieldName/binary, " = ">>,
                            {key, get_key_type((Type)), FieldArg}
                        ]
                    },
                ContentIn =
                    {
                        require,
                        [{notin, <<FieldArg/binary, "[in]">>, [null, undefined, 0]}],
                        [
                            <<" AND t1.", FieldName/binary, " in (">>,
                            {foreach_spilt, <<",">>, {key, get_key_type(Type), <<FieldArg/binary, "[in]">>}},
                            <<")">>
                        ]
                    },
                IsVarchar =
                    case Type of
                        <<"varchar", _/binary>> ->
                            true;
                        <<"text">> ->
                            true;
                        _ ->
                            false
                    end,
                ContentLike =
                    case IsVarchar of
                        true ->
                            [{
                                require,
                                [{notin, <<FieldArg/binary, "[like]">>, [null, undefined, 0]}],
                                [
                                    <<" AND t1.", FieldName/binary, " like '%">>,
                                    {key, integer, <<FieldArg/binary, "[like]">>},
                                    <<"%'">>
                                ]
                            }];
                        _ ->
                            []
                    end,
                case lists:member(Type, [<<"timestamp">>, <<"datetime">>]) of
                    true  ->
                        FildArgs2 = <<FieldArg/binary, "[begin]">>,
                        Content2 =
                            {
                                require,
                                [{notin, FildArgs2, [null, undefined, 0]}],
                                [
                                    <<" AND t1.", FieldName/binary, " >= date_format(">>,
                                    {key, get_key_type((Type)), FildArgs2},
                                    <<", '%y%m%d')">>
                                ]
                            },
                        FildArgs3 = <<FieldArg/binary, "[end]">>,
                        Content3 =
                            {
                                require,
                                [{notin, FildArgs3, [null, undefined, 0]}],
                                [
                                    <<" AND t1.", FieldName/binary, " <= date_format(">>,
                                    {key, get_key_type((Type)), FildArgs3},
                                    <<", '%y%m%d')">>
                                ]
                            },
                        Acc ++ [Content, ContentIn] ++ ContentLike ++ [Content2, Content3];
                    _ ->
                        Acc ++ [Content, ContentIn] ++ ContentLike
                end
            end, [], FiledsInfo),

    OrderByMaps =
        lists:foldl(
            fun(#{<<"Field">> := FieldName}, Acc) ->
                Arg = alinkdata_dao:transform_k(FieldName),
                Acc#{Arg => FieldName}
            end, #{}, FiledsInfo),

    DaoInfoDetail1 =
        [
            {
                require,
                [{notin, <<"dataScope">>, [null, undefined, <<>>]}],
                [{key, sql, <<"dataScope">>}]
            }
        ],

    DaoInfoDetailJoin =
        [
            {
                require,
                [{notin, <<"nodeType">>, [null, undefined, <<>>]}],
                [
                    <<" AND t1.product = t2.id AND t2.node_type in (">>,
                    {foreach_spilt, <<",">>, {key, str, <<"nodeType">>}},
                    <<")">>
                ]
            }
        ],

    DaoInfoTail0 = [
        {
            require,
            [{in, <<"orderBy[desc]">>, maps:keys(OrderByMaps)}],
            [
                <<" order by ">>,
                {key, {map_value, OrderByMaps}, <<"orderBy[desc]">>},
                <<" desc ">>
            ]
        }
    ],
    DaoInfoTail1 = [
        {
            require,
            [{in, <<"orderBy[asc]">>, maps:keys(OrderByMaps)}],
            [
                <<" order by ">>,
                {key, {map_value, OrderByMaps}, <<"orderBy[asc]">>},
                <<" asc ">>
            ]
        }
    ],
    DaoInfo = DaoInfoHead ++ DaoInfoDetail0 ++ DaoInfoDetailJoin ++ DaoInfoDetail1 ++ DaoInfoTail0 ++ DaoInfoTail1,
    APiTable = get_api_table(<<"sys_device">>),
    {binary_to_atom(<<"QUERY_", APiTable/binary>>, latin1), DaoInfo, APiTable};
list_daos(Table, FiledsInfo) ->
    DaoInfoHead = [<<"select * from ", Table/binary, " where 1 = 1 ">>],
    DaoInfoDetail0 =
        lists:foldl(
            fun(#{
                <<"Default">> := _Default,
                <<"Extra">> := _Extra,
                <<"Field">> := FieldName,
                <<"Key">> := _Key,
                <<"Null">> := _IsNull,
                <<"Type">> := Type
            }, Acc) ->
                FieldArg = alinkdata_dao:transform_k(FieldName),
                Content =
                    {
                        require,
                        [{notin, FieldArg, [null, undefined, 0]}],
                        [
                            <<" AND ", FieldName/binary, " = ">>,
                            {key, get_key_type((Type)), FieldArg}
                        ]
                    },
                ContentIn =
                    {
                        require,
                        [{notin, <<FieldArg/binary, "[in]">>, [null, undefined, 0]}],
                        [
                            <<" AND ", FieldName/binary, " in (">>,
                            {foreach_spilt, <<",">>, {key, get_key_type(Type), <<FieldArg/binary, "[in]">>}},
                            <<")">>
                        ]
                    },
                IsVarchar =
                    case Type of
                        <<"varchar", _/binary>> ->
                            true;
                        <<"text">> ->
                            true;
                        _ ->
                            false
                    end,
                ContentLike =
                    case IsVarchar of
                        true ->
                            [{
                                require,
                                [{notin, <<FieldArg/binary, "[like]">>, [null, undefined, 0]}],
                                [
                                    <<" AND ", FieldName/binary, " like '%">>,
                                    {key, integer, <<FieldArg/binary, "[like]">>},
                                    <<"%'">>
                                ]
                            }];
                        _ ->
                            []
                    end,
                case lists:member(Type, [<<"timestamp">>, <<"datetime">>]) of
                    true  ->
                        FildArgs2 = <<FieldArg/binary, "[begin]">>,
                        Content2 =
                            {
                                require,
                                [{notin, FildArgs2, [null, undefined, 0]}],
                                [
                                    <<" AND ", FieldName/binary, " >= date_format(">>,
                                    {key, get_key_type((Type)), FildArgs2},
                                    <<", '%y%m%d')">>
                                ]
                            },
                        FildArgs3 = <<FieldArg/binary, "[end]">>,
                        Content3 =
                            {
                                require,
                                [{notin, FildArgs3, [null, undefined, 0]}],
                                [
                                    <<" AND ", FieldName/binary, " <= date_format(">>,
                                    {key, get_key_type((Type)), FildArgs3},
                                    <<", '%y%m%d')">>
                                ]
                            },
                        Acc ++ [Content, ContentIn] ++ ContentLike ++ [Content2, Content3];
                    _ ->
                        Acc ++ [Content, ContentIn] ++ ContentLike
                end
            end, [], FiledsInfo),

    OrderByMaps =
        lists:foldl(
            fun(#{<<"Field">> := FieldName}, Acc) ->
                Arg = alinkdata_dao:transform_k(FieldName),
                Acc#{Arg => FieldName}
            end, #{}, FiledsInfo),

    DaoInfoDetail1 =
        [
            {
                require,
                [{notin, <<"dataScope">>, [null, undefined, <<>>]}],
                [{key, sql, <<"dataScope">>}]
            }
        ],

    DaoInfoTail0 = [
        {
            require,
            [{in, <<"orderBy[desc]">>, maps:keys(OrderByMaps)}],
            [
                <<" order by ">>,
                {key, {map_value, OrderByMaps}, <<"orderBy[desc]">>},
                <<" desc ">>
            ]
        }
    ],
    DaoInfoTail1 = [
        {
            require,
            [{in, <<"orderBy[asc]">>, maps:keys(OrderByMaps)}],
            [
                <<" order by ">>,
                {key, {map_value, OrderByMaps}, <<"orderBy[asc]">>},
                <<" asc ">>
            ]
        }
    ],
    DaoInfo = DaoInfoHead ++ DaoInfoDetail0 ++ DaoInfoDetail1 ++ DaoInfoTail0 ++ DaoInfoTail1,
    APiTable = get_api_table(Table),
    {binary_to_atom(<<"QUERY_", APiTable/binary>>, latin1), DaoInfo, APiTable}.



left_join_daos(Table, FiledsInfo) ->
    SelectedFields =
        lists:foldl(
            fun(#{<<"Field">> := FieldName}, Acc) when Acc =/= <<>> ->
                <<Acc/binary, ", a.", FieldName/binary, " as 'a.", FieldName/binary, "'">>;
               (#{<<"Field">> := FieldName}, _Acc) ->
                <<"a.", FieldName/binary, " as 'a.", FieldName/binary, "'">>
        end, <<>>, FiledsInfo),
    DaoInfoHead = [<<"select ", SelectedFields/binary, ", b.* from ", Table/binary, " a ">>],
    DaoInfoLeftJoin =
        [
            <<"left join sys_">>,
            {key, integer, <<"leftJoin[table]">>},
            <<" b on a.">>,
            {key, integer, <<"leftJoin[aParam]">>},
            <<" = b.">>,
            {key, integer, <<"leftJoin[bParam]">>},
            <<" where 1 = 1 ">>
        ],
    DaoInfoDetail0 =
        lists:foldl(
            fun(#{
                <<"Default">> := _Default,
                <<"Extra">> := _Extra,
                <<"Field">> := FieldName,
                <<"Key">> := _Key,
                <<"Null">> := _IsNull,
                <<"Type">> := Type
            }, Acc) ->
                FieldArg = alinkdata_dao:transform_k(FieldName),
                Content =
                    {
                        require,
                        [{notin, FieldArg, [undefined, 0]}],
                        [
                            <<" AND a.", FieldName/binary, " = ">>,
                            {key, get_key_type((Type)), FieldArg}
                        ]
                    },
                case lists:member(Type, [<<"timestamp">>, <<"datetime">>]) of
                    true  ->
                        FildArgs2 = <<FieldArg/binary, "[begin]">>,
                        Content2 =
                            {
                                require,
                                [{notin, FildArgs2, [undefined, 0]}],
                                [
                                    <<" AND a.", FieldName/binary, " >= date_format(">>,
                                    {key, get_key_type((Type)), FildArgs2},
                                    <<", '%y%m%d')">>
                                ]
                            },
                        FildArgs3 = <<FieldArg/binary, "[end]">>,
                        Content3 =
                            {
                                require,
                                [{notin, FildArgs3, [undefined, 0]}],
                                [
                                    <<" AND a.", FieldName/binary, " <= date_format(">>,
                                    {key, get_key_type((Type)), FildArgs3},
                                    <<", '%y%m%d')">>
                                ]
                            },
                        Acc ++ [Content] ++ [Content2, Content3];
                    _ ->
                        Acc ++ [Content]
                end
            end, [], FiledsInfo),

    OrderByMaps =
        lists:foldl(
            fun(#{<<"Field">> := FieldName}, Acc) ->
                Arg = alinkdata_dao:transform_k(FieldName),
                Acc#{Arg => FieldName}
            end, #{}, FiledsInfo),

    DaoInfoTail0 = [
        {
            require,
            [{in, <<"orderBy[desc]">>, maps:keys(OrderByMaps)}],
            [
                <<" order by a.">>,
                {key, {map_value, OrderByMaps}, <<"orderBy[desc]">>},
                <<" desc ">>
            ]
        }
    ],
    DaoInfoTail1 = [
        {
            require,
            [{in, <<"orderBy[asc]">>, maps:keys(OrderByMaps)}],
            [
                <<" order by a.">>,
                {key, {map_value, OrderByMaps}, <<"orderBy[asc]">>},
                <<" asc ">>
            ]
        }
    ],
    DaoInfo = DaoInfoHead ++ DaoInfoLeftJoin ++ DaoInfoDetail0 ++ DaoInfoTail0 ++ DaoInfoTail1,
    APiTable = get_api_table(Table),
    {binary_to_atom(<<"LEFT_JOIN_QUERY_", APiTable/binary>>, latin1), DaoInfo, APiTable}.




get_key_type(<<"auto_increment">>) ->
    integer;
get_key_type(<<"bigint", _/binary>>) ->
    integer;
get_key_type(<<"int", _/binary>>) ->
    integer;
get_key_type(<<"float", _/binary>>) ->
    float;
get_key_type(<<"double", _/binary>>) ->
    float;
get_key_type(<<"tinyint", _/binary>>) ->
    integer;
get_key_type(<<"smallint", _/binary>>) ->
    integer;
get_key_type(<<"mediumint", _/binary>>) ->
    integer;
get_key_type(<<"bit", _/binary>>) ->
    integer;
get_key_type(_) ->
    str.

get_api_table(Table) ->
    binary:replace(Table, ?PREFIX, <<>>).




table_info(Pid, Table) ->
    {ok, Fields} = alinkdata:format_result_to_map(mysql:query(Pid, <<"describe ", Table/binary>>)),
    LikeFields =
        lists:foldl(
            fun(#{<<"Type">> := Type, <<"Field">> := F} = FI, Acc)  ->
                IsVarchar =
                    case Type of
                        <<"varchar", _/binary>> ->
                            true;
                        <<"text">> ->
                            true;
                        _ ->
                            false
                    end,
                case IsVarchar of
                    true ->
                        [FI#{<<"Field">> => <<F/binary, "[like]">>}|Acc];
                    _ ->
                        Acc
                end
        end, [], Fields),
    FieldsInfo = lists:map(fun field_info/1, Fields),
    [#{<<"Field">> := IdRawName}] =
        lists:filter(
            fun(#{<<"Key">> := Key}) ->
                Key =:= <<"PRI">>
            end, Fields),
    #{
        <<"tableName">> => Table,
        <<"alinkdata_generated">> => true,
        <<"id">> => alinkdata_dao:transform_k(IdRawName),
        <<"fields">> => FieldsInfo,
        <<"like_fields">> => lists:map(fun field_info/1, LikeFields)
    }.


field_info(Field) ->
    #{
        <<"Default">> := Default,
        <<"Extra">> := Extra,
        <<"Field">> := FieldName,
        <<"Key">> := _Key,
        <<"Null">> := IsNull,
        <<"Type">> := Type
    } = Field,
    #{
        <<"type">> => Type,
        <<"required">> => false,
        <<"name">> => FieldName,
        <<"isnull">> => (IsNull =:= <<"Yes">>),
        <<"default">> => Default,
        <<"auto_increment">> => (Extra =:= <<"auto_increment">>),
        <<"alias">> => alinkdata_dao:transform_k(FieldName)
    }.




system_table() ->
    [<<"gen_table">>,<<"gen_table_column">>,<<"sys_config">>,
        <<"sys_dept">>,<<"sys_dict_data">>,<<"sys_dict_type">>,
        <<"sys_job">>,<<"sys_job_log">>,<<"sys_logininfor">>,
        <<"sys_menu">>,<<"sys_notice">>,<<"sys_oper_log">>,
        <<"sys_post">>,<<"sys_role">>,<<"sys_role_dept">>,
        <<"sys_role_menu">>,<<"sys_user">>,<<"sys_user_post">>,
        <<"sys_user_role">>].