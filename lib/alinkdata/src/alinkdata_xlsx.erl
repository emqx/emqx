%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 28. 6月 2023 下午11:15
%%%-------------------------------------------------------------------
-module(alinkdata_xlsx).

%% API
-export([
    from_file_data/1,
    from_file_data/2,
    to_xlsx_file_content/3,
    file_template/2
]).

%%%===================================================================
%%% API
%%%===================================================================
from_file_data(ContentData) ->
    RowHandler =
        fun(_, [1 | Row], {_Headers, Contents}) ->
            Headers = lists:map(
                fun(S) when is_list(S) ->
                    unicode:characters_to_binary(S, utf8);
                   (S) ->
                    alinkutil_type:to_binary(S)
            end, Row),
            {next_row, {Headers, Contents}};
            (_, [_ | Row], {Headers, Contents}) ->
                case lists:all(fun(RPoint) -> RPoint =:= [] end, Row) of
                    true ->
                        {next_sheet, {Headers, Contents}};
                    _ ->
                        Content = lists:map(
                            fun(S) when is_list(S) ->
                                unicode:characters_to_binary(S, utf8);
                                (S) ->
                                    alinkutil_type:to_binary(S)
                            end, Row),
                        {next_row, {Headers, [Content|Contents]}}
                end
        end,

    case alinkdata_xlsx_reader:read(ContentData,{[], []},RowHandler) of
        {error,Reason}->
            logger:error("read xlsx error: ~p", [Reason]),
            {error, decode_error};
        {Headers, Contents} ->
            HeadersLength = length(Headers),
            TransData =
                lists:map(
                    fun(Content) ->
                        UsefulContent = lists:sublist(Content, HeadersLength),
                        maps:from_list(lists:zip(Headers, UsefulContent))
                end, Contents),
            {ok, TransData}
    end.

from_file_data(ContentData, Table) ->
    Doc = get_table_doc(Table),
    DataDoc =
        maps:fold(
            fun(K, V, Acc) ->
                Title = maps:get(<<"title">>, V, K),
                ValuesDoc = maps:get(<<"valesDoc">>, V, #{}),
                DataValuesDoc =
                    maps:fold(
                        fun(DK, DV, Acc1) ->
                            Acc1#{DV => DK}
                    end, #{}, ValuesDoc),
                Acc#{Title => V#{<<"valesDoc">> => DataValuesDoc, <<"key">> => K}}
        end, #{}, Doc),
    {ok, Data} = from_file_data(ContentData),
    lists:map(
        fun(DataPoint) ->
            maps:fold(
                fun(K, V, Acc) ->
                    KDoc = maps:get(K, DataDoc, #{}),
                    Key = maps:get(<<"key">>, KDoc, K),
                    ValuesDoc = maps:get(<<"valesDoc">>, KDoc, #{}),
                    NV = maps:get(alinkutil_type:to_binary(V), ValuesDoc, V),
                    Acc#{Key => NV}
            end, #{}, DataPoint)
    end, Data).




to_xlsx_file_content(FileName, Table, Content) when is_binary(Table) ->
    Doc = get_table_doc(Table),
    to_xlsx_file_content(FileName, Doc, Content);
to_xlsx_file_content(FileName, Doc, Content) when is_map(Doc) ->
    XlsxFileName = alinkutil_type:to_list(FileName),
    XlsxHandle = alinkdata_xlsx_writer:create(XlsxFileName, <<"有智云"/utf8>>),
    SheetContent = to_sheet_content(Doc, Content),
    SheetHandle = alinkdata_xlsx_writer:create_sheet(XlsxFileName, SheetContent),
    alinkdata_xlsx_writer:add_sheet(XlsxHandle,SheetHandle),
    case alinkdata_xlsx_writer:close(XlsxHandle) of
        {ok, {_FileN, FileBin}} ->
            {ok, FileBin};
        {error, Reason} ->
            {error, Reason}
    end.


file_template(FileName, Table) when is_binary(Table) ->
    Doc = get_table_doc(Table),
    file_template(FileName, Doc);
file_template(FileName, Doc) when is_map(Doc) ->
    XlsxFileName = alinkutil_type:to_list(FileName),
    XlsxHandle = alinkdata_xlsx_writer:create(XlsxFileName, <<"有智云"/utf8>>),
    SheetContent = to_sheet_header_content(Doc),
    SheetHandle = alinkdata_xlsx_writer:create_sheet(XlsxFileName, SheetContent),
    alinkdata_xlsx_writer:add_sheet(XlsxHandle,SheetHandle),
    case alinkdata_xlsx_writer:close(XlsxHandle) of
        {ok, {_FileN, FileBin}} ->
            {ok, FileBin};
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================
to_sheet_header_content(Doc) ->
    Ks = maps:keys(Doc),
    HeadersSheetContent =
        lists:map(
            fun(K) ->
                KDoc = maps:get(K, Doc, #{}),
                maps:get(<<"title">>, KDoc, K)
            end, Ks),
    [[1|HeadersSheetContent]].



to_sheet_content(_Doc, []) ->
    [];
to_sheet_content(Doc, Content) ->
    Ks = maps:keys(Doc),
    HeadersSheetContent =
        lists:map(
            fun(K) ->
                KDoc = maps:get(K, Doc, #{}),
                maps:get(<<"title">>, KDoc, K)
        end, Ks),
    {_, SheetContent} =
        lists:foldl(
            fun(ContentPoint, {Seq, Acc0}) ->
                Line = lists:foldl(
                    fun(K, Acc) ->
                        KDoc = maps:get(K, Doc, #{}),
                        Precision = maps:get(<<"precision">>, KDoc, 0),
                        Type = maps:get(<<"type">>, KDoc, <<>>),
                        Values = maps:get(<<"valuesDoc">>, KDoc, #{}),
                        Default = maps:get(<<"default">>, KDoc, <<>>),
                        V =
                            case maps:get(K, ContentPoint, Default) of
                                KV when is_float(KV) andalso Precision > 0 ->
                                    float_to_binary(KV, [{decimals, Precision}]);
                                KV when Type =:= <<"datetime">> andalso is_integer(KV) ->
                                    alinkdata_wechat:timestamp2localtime_str(KV);
                                KV ->
                                    maps:get(alinkutil_type:to_binary(KV), Values, KV)
                            end,
                        Acc ++ [V]
                 end, [Seq], Ks),
                {Seq + 1, [Line|Acc0]}
        end, {2, []}, Content),
    [[1|HeadersSheetContent] | SheetContent].


get_table_doc(<<"device_data_", Addr/binary>>) ->
    case alinkcore_cache:query_device(Addr) of
        {ok, #{<<"product">> := ProductId}} ->
            case alinkcore_cache:query_product(ProductId) of
                {ok, #{<<"thing">> := Things}} ->
                    ThingMap =
                        lists:foldl(
                            fun(#{<<"name">> := Name} = ThingPOint, Acc) ->
                                NameLow = alinkutil_type:to_binary(string:to_lower(alinkutil_type:to_list(Name))),
                                Acc#{NameLow => #{
                                    <<"title">> => maps:get(<<"title">>, ThingPOint, Name),
                                    <<"precision">> => maps:get(<<"precision">>, ThingPOint, 2)}}
                        end, #{}, Things),
                    ThingMap#{
                        <<"addr">> => #{<<"title">> => <<"设备地址"/utf8>>, <<"default">> => Addr},
                        <<"createtime">> => #{<<"title">> => <<"上报时间"/utf8>>, <<"type">> => <<"datetime">>}
                    };
                {error, _} ->
                    #{}
            end;
        _ ->
            #{}
    end;
get_table_doc(Table) ->
    FileName = code:priv_dir(alinkdata) ++ "/export_desc/" ++ alinkutil_type:to_list(Table) ++ ".json",
    case filelib:is_file(FileName) of
        true ->
            {ok, FileContent} = file:read_file(FileName),
            RDoc = jiffy:decode(FileContent, [return_maps]),
            maps:fold(
                fun(K, #{<<"valuesDictKey">> := DictKey} = V, Acc) ->
                    case alinkdata_dao:query_no_count(select_dict_data_list, #{<<"dictType">> => DictKey}) of
                        {ok, []} ->
                            Acc;
                        {ok, Vs} ->
                            ValuesDocs = maps:get(<<"valuesDoc">>, RDoc, #{}),
                            NValuesDoc =
                                lists:foldl(
                                    fun(#{<<"dictValue">> := DictK, <<"dictLabel">> := DictV}, Acc1) ->
                                        Acc1#{DictK => DictV}
                                end, ValuesDocs, Vs),
                            Acc#{K => V#{<<"valuesDoc">> => NValuesDoc}}
                    end;
                   (K, V, Acc) ->
                    Acc#{K => V}
            end, #{}, RDoc);
        _ ->
            #{}
    end.

