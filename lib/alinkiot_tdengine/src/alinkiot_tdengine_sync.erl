%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 22. 3月 2023 上午1:19
%%%-------------------------------------------------------------------
-module(alinkiot_tdengine_sync).
-author("yqfclid").

%% API
-export([
    sync/0,
    sync/1,
    sync_add/0,
    all_device_table_add_tag/2,
    all_device_table_delete_tag/1,
    all_device_table_add_column/2,
    all_device_table_delete_column/1
]).

-define(IS_EMPTY(V), V == <<>> orelse V == null orelse V == undefined).

%%%===================================================================
%%% API
%%%===================================================================
sync() ->
    case alinkdata_dao:query_no_count('QUERY_product', #{}) of
        {ok, Products} ->
            lists:foreach(
                fun(#{<<"id">> := ProductId, <<"thing">> := Thing}) when not (?IS_EMPTY(Thing)) ->
                    repaire(ProductId, jiffy:decode(Thing, [return_maps]));
                   (_) ->
                    ok
            end, Products);
        {error, Reason} ->
            logger:error("sync product info failed:~p", [Reason])
    end.

sync(ProductId) ->
    case alinkdata_dao:query_no_count('QUERY_product', #{<<"id">> => ProductId}) of
        {ok, [#{<<"thing">> := Thing}]} when not (?IS_EMPTY(Thing)) ->
            repaire(ProductId, jiffy:decode(Thing, [return_maps]));
        {ok, _} ->
            ok;
        {error, Reason} ->
            logger:error("sync product info failed:~p", [Reason])
    end.

sync_add() ->
    case alinkdata_dao:query_no_count('QUERY_product', #{}) of
        {ok, Products} ->
            lists:foreach(
                fun(#{<<"id">> := ProductId, <<"thing">> := Thing}) when not (?IS_EMPTY(Thing))  ->
                    check_add(ProductId, jiffy:decode(Thing, [return_maps]));
                    (_) ->
                        ok
                end, Products);
        {error, Reason} ->
            logger:error("sync product info failed:~p", [Reason])
    end.


all_device_table_add_tag(Name, Type) ->
    TagType = alinkiot_tdengine_common:transform_field_type(Type),
    case alinkiot_tdengine:request(<<"show stables">>) of
        {ok, _, ExistsSTables} ->
            ExistsSTablesList0 = lists:map(fun(#{<<"stable_name">> := T}) ->  T end, ExistsSTables),
            ExistsSTablesList =
                lists:filter(fun(<<"metrics_", _/binary>>) -> false;
                    (_) -> true end, ExistsSTablesList0),
            lists:foreach(
                fun(STable) ->
                    case alinkiot_tdengine:request(<<"describe ", STable/binary>>) of
                        {ok, _, Schema} ->
                                case lists:any(
                                    fun(#{<<"field">> := N}) ->
                                        Name =:= N
                                    end, Schema) of
                                    true ->
                                        ok;
                                    _ ->
                                        TagSql = <<"alter stable ", STable/binary, " add tag ",
                                            Name/binary, " ", TagType/binary>>,
                                        case alinkiot_tdengine:request(TagSql) of
                                            {ok, _, _} ->
                                                ok;
                                            {error, Reason} ->
                                                logger:error("add ~p tag failed ~p", [STable, Reason])
                                        end
                                end;
                        {error, Reason} ->
                            logger:error("descirbe ~p failed ~p", [STable, Reason])
                    end
            end, ExistsSTablesList);
        {error, Reason} ->
            logger:error("get stables failed ~p", [Reason])
    end.


all_device_table_delete_tag(Name) ->
    case alinkiot_tdengine:request(<<"show stables">>) of
        {ok, _, ExistsSTables} ->
            ExistsSTablesList0 = lists:map(fun(#{<<"stable_name">> := T}) ->  T end, ExistsSTables),
            ExistsSTablesList =
                lists:filter(fun(<<"metrics_", _/binary>>) -> false;
                    (_) -> true end, ExistsSTablesList0),
            lists:foreach(
                fun(STable) ->
                    case alinkiot_tdengine:request(<<"describe ", STable/binary>>) of
                        {ok, _, Schema} ->
                            case lists:any(
                                fun(#{<<"field">> := N}) ->
                                    Name =:= N
                                end, Schema) of
                                true ->
                                    TagSql = <<"alter stable ", STable/binary, " drop tag ", Name/binary>>,
                                    case alinkiot_tdengine:request(TagSql) of
                                        {ok, _, _} ->
                                            ok;
                                        {error, Reason} ->
                                            logger:error("add ~p tag failed ~p", [STable, Reason])
                                    end;
                                _ ->
                                    ok
                            end;
                        {error, Reason} ->
                            logger:error("descirbe ~p failed ~p", [STable, Reason])
                    end
                end, ExistsSTablesList);
        {error, Reason} ->
            logger:error("get stables failed ~p", [Reason])
    end.


all_device_table_add_column(Name, Type) ->
    TagType = alinkiot_tdengine_common:transform_field_type(Type),
    case alinkiot_tdengine:request(<<"show stables">>) of
        {ok, _, ExistsSTables} ->
            ExistsSTablesList0 = lists:map(fun(#{<<"stable_name">> := T}) ->  T end, ExistsSTables),
            ExistsSTablesList =
                lists:filter(fun(<<"metrics_", _/binary>>) -> false;
                    (_) -> true end, ExistsSTablesList0),
            lists:foreach(
                fun(STable) ->
                    case alinkiot_tdengine:request(<<"describe ", STable/binary>>) of
                        {ok, _, Schema} ->
                            case lists:any(
                                fun(#{<<"field">> := N}) ->
                                    Name =:= N
                                end, Schema) of
                                true ->
                                    ok;
                                _ ->
                                    TagSql = <<"alter stable ", STable/binary, " add column ",
                                        Name/binary, " ", TagType/binary>>,
                                    case alinkiot_tdengine:request(TagSql) of
                                        {ok, _, _} ->
                                            ok;
                                        {error, Reason} ->
                                            logger:error("add ~p tag failed ~p", [STable, Reason])
                                    end
                            end;
                        {error, Reason} ->
                            logger:error("descirbe ~p failed ~p", [STable, Reason])
                    end
                end, ExistsSTablesList);
        {error, Reason} ->
            logger:error("get stables failed ~p", [Reason])
    end.


all_device_table_delete_column(Name) ->
    case alinkiot_tdengine:request(<<"show stables">>) of
        {ok, _, ExistsSTables} ->
            ExistsSTablesList0 = lists:map(fun(#{<<"stable_name">> := T}) ->  T end, ExistsSTables),
            ExistsSTablesList =
                lists:filter(fun(<<"metrics_", _/binary>>) -> false;
                    (_) -> true end, ExistsSTablesList0),
            lists:foreach(
                fun(STable) ->
                    case alinkiot_tdengine:request(<<"describe ", STable/binary>>) of
                        {ok, _, Schema} ->
                            case lists:any(
                                fun(#{<<"field">> := N}) ->
                                    Name =:= N
                                end, Schema) of
                                true ->
                                    TagSql = <<"alter stable ", STable/binary, " drop column ", Name/binary>>,
                                    case alinkiot_tdengine:request(TagSql) of
                                        {ok, _, _} ->
                                            ok;
                                        {error, Reason} ->
                                            logger:error("add ~p tag failed ~p", [STable, Reason])
                                    end;
                                _ ->
                                    ok
                            end;
                        {error, Reason} ->
                            logger:error("descirbe ~p failed ~p", [STable, Reason])
                    end
                end, ExistsSTablesList);
        {error, Reason} ->
            logger:error("get stables failed ~p", [Reason])
    end.
%%%===================================================================
%%% Internal functions
%%%===================================================================
check_add(ProductId, RThingInfo) ->
    ThingInfo = lowercase_thing(RThingInfo),
    ProductIdB = alinkutil_type:to_binary(ProductId),
    STable = <<"alinkiot_", ProductIdB/binary>>,
    case alinkiot_tdengine:request(<<"show stables">>) of
        {ok, _, ExistsSTables} ->
            ExistsSTablesList0 = lists:map(fun(#{<<"stable_name">> := T}) ->  T end, ExistsSTables),
            ExistsSTablesList =
                lists:filter(fun(<<"metrics_", _/binary>>) -> false;
                    (_) -> true end, ExistsSTablesList0),
            case lists:member(STable, ExistsSTablesList) of
                true ->
                    ok;
                false ->
                    alinkiot_tdengine:create_alink_schema(ProductId, ThingInfo)
            end;
        {error, Reason} ->
            {error, Reason}
    end.


repaire(ProductId, []) ->
    ProductIdB = alinkutil_type:to_binary(ProductId),
    STable = <<"alinkiot_", ProductIdB/binary>>,
    case alinkiot_tdengine:drop_stable(STable) of
        {ok, _, _} ->
            ok;
        {error, Reason} ->
            logger:error("drop stable ~p failed ~p", [STable, Reason])
    end;
repaire(ProductId, RThingInfo) ->
    ThingInfo = lowercase_thing(RThingInfo),
    ProductIdB = alinkutil_type:to_binary(ProductId),
    STable = <<"alinkiot_", ProductIdB/binary>>,
    case get_stable_schema(STable) of
        {ok, Schema, ValueFieldsCount} when ValueFieldsCount > 1 ->
            {AddColumns, DelColumns} = diff_schema_and_thing(Schema, ThingInfo),
            alinkiot_tdengine:del_columns(STable, DelColumns),
            alinkiot_tdengine:add_columns(STable, AddColumns);
        {ok, Schema, _ValueFieldsCount} ->
            {AddColumns, DelColumns} = diff_schema_and_thing(Schema, ThingInfo),
            case length(DelColumns) > 0 of
                true ->
                    alinkiot_tdengine:drop_stable(STable),
                    alinkiot_tdengine:create_alink_schema(ProductId, ThingInfo);
                false ->
                    alinkiot_tdengine:del_columns(STable, DelColumns),
                    alinkiot_tdengine:add_columns(STable, AddColumns)
            end;
        {error, not_found} ->
            alinkiot_tdengine:create_alink_schema(ProductId, ThingInfo);
        {error, Reason} ->
            logger:error("repaire failed ~p", [Reason])
    end.



get_stable_schema(STable) ->
    case alinkiot_tdengine:request(<<"show stables">>) of
        {ok, _, ExistsSTables} ->
            ExistsSTablesList0 = lists:map(fun(#{<<"stable_name">> := T}) ->  T end, ExistsSTables),
            ExistsSTablesList =
                lists:filter(fun(<<"metrics_", _/binary>>) -> false;
                    (_) -> true end, ExistsSTablesList0),
            case lists:member(STable, ExistsSTablesList) of
                true ->
                    case alinkiot_tdengine:request(<<"describe ", STable/binary>>) of
                        {ok, _, Schema} ->
                            ValueFieldsCount =
                                length(lists:filter(
                                    fun(#{<<"field">> := Name, <<"note">> := Note}) ->
                                        Name =/= <<"time">> andalso
                                            Note =/= <<"TAG">>
                                            andalso Name =/= <<"alarm_rule">>
                                end, Schema)),
                            {ok, Schema, ValueFieldsCount};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                false ->
                    {error, not_found}
            end;
        {error, Reason} ->
            {error, Reason}
    end.



diff_schema_and_thing(Schema, ThingInfo) ->
    AddColmns =
        lists:foldl(
            fun(#{<<"name">> := Name, <<"type">> := _Type} = Thing, Acc) when Name =/= <<"time">> ->
                TdF = alinkiot_tdengine_common:to_td_field_name(Name),
                ThingType = alinkiot_tdengine_common:thing_field_precision_type(Thing),
                IsExistInSchema =
                    lists:any(
                        fun(#{<<"field">> := Field,
                              <<"type">> := FieldType,
                              <<"note">> := Note}) when Field =:= TdF
                                                   andalso Note =/= <<"TAG">>
                                                    andalso Field =/= <<"alarm_rule">> ->
                            case alinkiot_tdengine_common:transform_field_type(ThingType) of
                                FieldType ->
                                    true;
                                _ ->
                                    false
                            end;
                           (_) ->
                               false
                    end, Schema),
                case IsExistInSchema of
                    true ->
                        Acc;
                    false ->
                        [#{<<"name">> => TdF, <<"type">> => ThingType}|Acc]
                end;
               (_, Acc) ->
                   Acc
        end, [], ThingInfo),
    DelColumns =
        lists:foldl(
            fun(#{<<"field">> := TdF,
                  <<"type">> := FieldType,
                  <<"note">> := Note}, Acc) when TdF =/= <<"time">>
                                            andalso Note =/= <<"TAG">>
                                            andalso TdF =/= <<"alarm_rule">> ->
                Field = alinkiot_tdengine_common:from_td_field_name(TdF),
                IsExistInThing =
                    lists:any(
                        fun(#{<<"name">> := Name, <<"type">> := _Type} = Thing) when Name =:= Field
                                                                        andalso Field =/= TdF ->
                            ThingType = alinkiot_tdengine_common:thing_field_precision_type(Thing),
                            case alinkiot_tdengine_common:transform_field_type(ThingType) of
                                FieldType ->
                                    true;
                                _ ->
                                    false
                            end;
                            (_) ->
                                false
                        end, ThingInfo),
                case IsExistInThing of
                    true ->
                        Acc;
                    false ->
                        [TdF|Acc]
                end;
                (_, Acc) ->
                    Acc
            end, [], Schema),
    {AddColmns, DelColumns}.


lowercase_thing(RThingInfo) ->
    lists:map(
        fun(#{<<"name">> := Name} = Thing) ->
            NName = list_to_binary(string:to_lower(binary_to_list(Name))),
            Thing#{<<"name">> => NName}
    end, RThingInfo).
