%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 21. 7月 2023 下午5:08
%%%-------------------------------------------------------------------
-module(alinkdata_xlsx_reader).
-author("yqfclid").

-include_lib("xmerl/include/xmerl.hrl").

%% API
-export([read/3]).

-record(xlsx_sheet, {id, name}).
-record(xlsx_share, {id, string}).
-record(xlsx_relation, {id, target, type}).

-spec(read(XlsxFile :: string(),InputContext::term(), RowHandler :: atom() | function()) ->
    {error, Resaon :: atom()} | {error, Resaon :: string()} |
    {ok}).

read(XlsxFile,InputContext, RowHandler) ->
    case zip:zip_open(XlsxFile, [memory]) of
        {error, Reason} -> {error, Reason};
        {ok, ZipHandle} ->
            Result= read_memory(ZipHandle,InputContext, RowHandler),
            clear_xlsx_context(),
            zip:zip_close(ZipHandle),
            Result
    end.

read_memory(XlsxZipHandle, InputContext,RowHandler) ->
    try
        process_sheetinfos(XlsxZipHandle),
        process_sharestring(XlsxZipHandle),
        process_relationhips(XlsxZipHandle),
        {_,OutContext} = process_sheet_table(XlsxZipHandle,InputContext, RowHandler),
        OutContext
    catch
        error:Reason -> {error, Reason}
    end.

process_sharestring(ZipHandle) ->
    ShareStringFile = "xl/sharedStrings.xml",
    case zip:zip_get(ShareStringFile, ZipHandle) of
        {error, Reason} ->
            ErrorString = alinkdata_xlsx_util:sprintf("zip:zip_get ~p error:~p", [ShareStringFile, Reason]);
        {ok, ShareStrings} ->
            {_File, Binary} = ShareStrings,
            do_put_shareString(Binary)
    end.


process_relationhips(ZipHandle) ->
    RelationFile = "xl/_rels/workbook.xml.rels",
    case zip:zip_get(RelationFile, ZipHandle) of
        {error, Reason} ->
            ErrorString = alinkdata_xlsx_util:sprintf("zip:zip_get ~p error:~p", [RelationFile, Reason]),
            error(ErrorString);
        {ok, Relationships} ->
            {_File, Binary} = Relationships,
            case xmerl_scan:string(binary_to_list(Binary)) of
                {ParsedDocRootEl, _Rest} ->
                    Result = xmerl_xpath:string("Relationship ", ParsedDocRootEl),
                    lists:foreach(fun(Relation) -> put_xlsx_relation(get_relationship_info(Relation)) end, Result);
                ExceptResult -> ErrorString = alinkdata_xlsx_util:sprintf("xmerl_scan:string error:~p", ExceptResult),
                    error(ErrorString)
            end
    end.
process_sheet_table(XlsxZipHandle,InputContext, RowHandler) ->
    lists:foldl(
        fun (_SheetInfo,{break,Context}) ->
            {break,Context};
            (SheetInfo,{next_sheet,Context}) ->
                SheetId = SheetInfo#xlsx_sheet.id,
                SheetName = SheetInfo#xlsx_sheet.name,
                Relation = get_xlsx_relation(SheetId),
                TargetFile = "xl/" ++ Relation#xlsx_relation.target,
                case zip:zip_get(TargetFile, XlsxZipHandle) of
                    {error, Reason} -> ErrorString = alinkdata_xlsx_util:sprintf("zip:zip_get ~p error:~p", [TargetFile, Reason]),error(ErrorString);
                    {ok, {_FileName, SheetBinary}} ->
                        process_sheet(SheetBinary, SheetName,RowHandler,Context)
                end
        end, {next_sheet,InputContext},get_xlsx_sheets()).


process_sheet(SheetBinary, SheetName ,RowHandler,InputContext) ->
    case xmerl_scan:string(binary_to_list(SheetBinary)) of
        {ParsedDocRootEl, _Rest} ->
            XMLS = ParsedDocRootEl#xmlElement.content,
%%            Dimensions = alinkdata_xlsx_util:xmlElement_from_name('dimension', XMLS),
%%            ColumnCount = alinkdata_xlsx_util:get_column_count(alinkdata_xlsx_util:xmlattribute_value_from_name('ref', Dimensions)),
            XmlSheetDatas = alinkdata_xlsx_util:xmlElement_from_name('sheetData', XMLS),
            XmlRows = lists:filter(
                fun(XmlRow) ->
                    case alinkdata_xlsx_util:is_record(XmlRow, 'xmlElement') of
                        false -> false;
                        true -> XmlRow#xmlElement.name =:= 'row'
                    end
                end, XmlSheetDatas#xmlElement.content),

            DefData = lists:duplicate(30, ""),
            RHRes = lists:foldl(
                fun(_XmlRow, {next_sheet,Context}) ->
                    {next_sheet,Context};
                    (_XmlRow, {break,Context}) ->
                        {break,Context};
                    (XmlRow, {next_row,Context}) ->
                        case get_row(XmlRow, DefData) of
                            [] -> {next_row,Context};
                            NewData ->
                                RowHandler(SheetName, NewData ,Context)
                        end
                end, {next_row,InputContext}, XmlRows),
            case RHRes of
                {next_row,OutContext}-> {next_sheet,OutContext};
                _-> RHRes
            end;
        ExceptResult ->
            ErrorString = alinkdata_xlsx_util:sprintf("xmerl_scan:string error:~p", ExceptResult),
            error(ErrorString)
    end.

do_put_shareString(BinaryString) ->
    case xmerl_scan:string(binary_to_list(BinaryString)) of
        {ParsedDocRootEl, _Rest} ->
            XMLS = ParsedDocRootEl#xmlElement.content,
            FltNodes = lists:filter(
                fun(Node) ->
                    element(#xmlElement.name, Node) =:= 'si'
                end, XMLS),
            TextList = lists:map(
                fun(Node) ->
                    Elements = Node#xmlElement.content,
                    lists:foldl(
                        fun(Element, AccTxt) ->
                            case alinkdata_xlsx_util:is_record(Element, xmlElement) of
                                false -> AccTxt;
                                true ->
                                    case Element#xmlElement.name of
                                        t -> get_element_text(Element);
                                        r ->
                                            TxtNode = alinkdata_xlsx_util:xmlElement_from_name('t', Element#xmlElement.content),
                                            AccTxt ++ get_element_text(TxtNode);
                                        _ ->
                                            AccTxt
                                    end
                            end
                        end, [], Elements)
                end, FltNodes),
            lists:foldl(
                fun(Txt, Id) ->
                    put_xlsx_share(Id, Txt),
                    Id + 1
                end, 0, TextList);
        ExceptResult ->
            ErrorString = alinkdata_xlsx_util:sprintf("xmerl_scan:string error:~p", ExceptResult),
            error(ErrorString)
    end.



get_element_text(Element) ->
    IsRecord = alinkdata_xlsx_util:is_record(Element, xmlElement),
    if not IsRecord -> [];
        true ->
            if element(#xmlElement.name, Element) =:= 't' ->

                TextNodes = lists:filter(
                    fun(Text) ->
                        alinkdata_xlsx_util:is_record(Text, xmlText)
                    end,
                    Element#xmlElement.content),

                case TextNodes of
                    [] -> [];
                    _ -> lists:flatmap(
                        fun(T) ->
                            Text1 = element(#xmlText.value, T),
                            Text1
                        end,
                        TextNodes)
                end
            end
    end.
process_sheetinfos(ZipHandle) ->
    SheetFile = "xl/workbook.xml",
    case zip:zip_get(SheetFile, ZipHandle) of
        {error, Reason} ->
            ErrorString = alinkdata_xlsx_util:sprintf("zip:zip_get ~p error:~p", [SheetFile, Reason]),
            error(ErrorString);
        {ok, WorkBook} ->
            {_File, Binary} = WorkBook,
            case xmerl_scan:string(binary_to_list(Binary)) of
                {ParsedDocRootEl, _Rest} ->
                    SheetInfos = xmerl_xpath:string("//sheet", ParsedDocRootEl),
                    lists:foreach(
                        fun(SheetInfoRaw) ->
                            SheetInfo = sheetinfo_from_workbook(SheetInfoRaw),
                            put_xlsx_sheet(SheetInfo)
                        end, SheetInfos),
                    ok;
                ExceptResult ->
                    ErrorString = alinkdata_xlsx_util:sprintf("xmerl_scan:string error:~p", ExceptResult),
                    error(ErrorString)
            end
    end.


sheetinfo_from_workbook(SheetInfo) ->
    Attributes = SheetInfo#xmlElement.attributes,
    lists:foldl(
        fun(Attr, Acc) ->
            if Attr#xmlAttribute.name =:= 'r:id' ->
                Acc#xlsx_sheet{id = Attr#xmlAttribute.value};
                Attr#xmlAttribute.name =:= 'name' ->
                    Acc#xlsx_sheet{name = Attr#xmlAttribute.value};
                true ->
                    Acc
            end
        end, #xlsx_sheet{}, Attributes).

get_relationship_info(Relation) ->
    Attributes = Relation#xmlElement.attributes,
    lists:foldl(fun(Attr, Acc) ->
        if Attr#xmlAttribute.name =:= 'Target' ->
            Acc#xlsx_relation{target = Attr#xmlAttribute.value};
            Attr#xmlAttribute.name =:= 'Id' ->
                Acc#xlsx_relation{id = Attr#xmlAttribute.value};
            Attr#xmlAttribute.name =:= 'Type' ->
                Acc#xlsx_relation{type = filename:basename(Attr#xmlAttribute.value)};
            true ->
                Acc
        end
                end, #xlsx_relation{}, Attributes).


get_row(XmlRow, DefaultList) ->
    XmlCells = lists:filter(
        fun(XmlCell) ->
            case alinkdata_xlsx_util:is_record(XmlCell, 'xmlElement') of
                false -> false;
                true -> XmlCell#xmlElement.name =:= 'c'
            end
        end, XmlRow#xmlElement.content),
    case XmlCells of
        [] -> [];
        [XmlCell1 | LeftXmlCells] ->
            {CellName1, CellValue1} = get_cell_info(XmlCell1),
            {X1, Y1} = alinkdata_xlsx_util:get_field_number(CellName1),
            RowDataWithKey = alinkdata_xlsx_util:take_nth_list(1, DefaultList, Y1),
            RowData1 = alinkdata_xlsx_util:take_nth_list(X1 + 1, RowDataWithKey, CellValue1),
            lists:foldl(
                fun(Cell, RowData) ->
                    {CellName, CellValue} = get_cell_info(Cell),
                    {X, _Y} = alinkdata_xlsx_util:get_field_number(CellName),
                    alinkdata_xlsx_util:take_nth_list(X + 1, RowData, CellValue)
                end, RowData1, LeftXmlCells)
    end.

get_subnode_text('is',XmlNode) ->
    case lists:keyfind('is', #xmlElement.name, XmlNode#xmlElement.content) of
%%        false -> [];
        false ->
            case XmlNode#xmlElement.content of
                [] -> [];
                XmlTexts ->
                    NewList =
                        lists:foldl(
                            fun(T, Acc) when is_record(T, xmlText)->
                                Bin = unicode:characters_to_binary(T#xmlText.value, utf8),
                                [binary_to_list(Bin)|Acc];
                                (SubE, Acc) when is_record(SubE, xmlElement)->
                                    case SubE#xmlElement.content of
                                        [] ->
                                            Acc;
                                        _ ->
                                            T = hd(SubE#xmlElement.content),
                                            Bin = unicode:characters_to_binary(T#xmlText.value, utf8),
                                            [binary_to_list(Bin)|Acc]
                                    end
                            end, [], XmlTexts),
                    alinkdata_xlsx_util:str_normalize(lists:flatten(NewList))
            end;
        SubNode ->
            case SubNode#xmlElement.content of
                [] -> [];
                XmlTexts ->
                    NewList =
                        lists:foldl(
                            fun(T, Acc) when is_record(T, xmlText)->
                                Bin = unicode:characters_to_binary(T#xmlText.value, utf8),
                                [binary_to_list(Bin)|Acc];
                               (SubE, Acc) when is_record(SubE, xmlElement)->
                                case SubE#xmlElement.content of
                                    [] ->
                                        Acc;
                                    _ ->
                                        T = hd(SubE#xmlElement.content),
                                        Bin = unicode:characters_to_binary(T#xmlText.value, utf8),
                                        [binary_to_list(Bin)|Acc]
                                end
                            end, [], XmlTexts),
                    alinkdata_xlsx_util:str_normalize(lists:flatten(NewList))
            end
    end;
get_subnode_text(SubName,XmlNode) ->
    case lists:keyfind(SubName, #xmlElement.name, XmlNode#xmlElement.content) of
        false -> [];
        SubNode ->
            case SubNode#xmlElement.content of
                [] -> [];
                XmlTexts ->
                    NewList =
                        lists:map(
                            fun(T) ->
                                Bin = unicode:characters_to_binary(T#xmlText.value, utf8),
                                binary_to_list(Bin)
                            end, XmlTexts),
                    alinkdata_xlsx_util:str_normalize(lists:flatten(NewList))
            end
    end.



get_cell_info(XmlCell) ->
    CellName = alinkdata_xlsx_util:xmlattribute_value_from_name('r', XmlCell),
    CellValue = case alinkdata_xlsx_util:has_attribute_value( 't', "s",XmlCell) of %% 1: t="s" => shareString | 2: t="str" => <v>String</v>
                    true ->
                        NewCellValue = list_to_integer(get_subnode_text('v',XmlCell)),
                        get_xlsx_share_string(NewCellValue);
                    false ->
                        case alinkdata_xlsx_util:has_attribute_value( 't', "b",XmlCell) of
                            true-> case list_to_integer(get_subnode_text('v',XmlCell)) of
                                       1-> "TRUE";
                                       _-> "FALSE"
                                   end;
                            false->get_subnode_text('is',XmlCell)
                        end
                end,
    {CellName, CellValue}.

%%
%% xlsx inner helper
%%
put_xlsx_sheet(SheetInfo) ->
    case get({xlsx_inner_context, sheet}) of
        undefined -> put({xlsx_inner_context, sheet}, [SheetInfo]);
        SheetInfos -> put({xlsx_inner_context, sheet}, SheetInfos ++ [SheetInfo])
    end.

get_xlsx_sheets() ->
    case get({xlsx_inner_context, sheet}) of
        undefined -> [];
        SheetInfos -> SheetInfos
    end.

get_xlsx_share_table() ->
    case get({xlsx_inner_context, share}) of
        undefined ->
            Tab = ets:new(share_string, [set, {keypos, #xlsx_share.id}]),
            put({xlsx_inner_context, share},Tab),
            Tab;
        Tab ->
            Tab
    end.

put_xlsx_share(Id, String) ->
    Tab = get_xlsx_share_table(),
    ets:insert(Tab, #xlsx_share{id = Id, string = String}).

get_xlsx_share_string(Id) ->
    Tab = get_xlsx_share_table(),
    case ets:lookup(Tab, Id) of
        [] -> "";
        [Obj] -> Obj#xlsx_share.string
    end.
%%%%%%%%%
put_xlsx_relation(Relation) ->
    case get({xlsx_inner_context, relation}) of
        undefined -> put({xlsx_inner_context, relation}, [Relation]);
        OldRelations -> put({xlsx_inner_context, relation}, [Relation | OldRelations])
    end.

get_xlsx_relation(Id) ->
    case get({xlsx_inner_context, relation}) of
        undefined -> false;
        Relations -> lists:keyfind(Id, #xlsx_relation.id, Relations)
    end.
clear_xlsx_context() ->
    erlang:erase({xlsx_inner_context, sheet}),
    Tab = get_xlsx_share_table(),
    ets:delete(Tab),
    erlang:erase({xlsx_inner_context,relation}),
    erlang:erase({xlsx_inner_context, share}).