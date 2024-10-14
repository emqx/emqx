%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 21. 7月 2023 下午5:02
%%%-------------------------------------------------------------------
-module(alinkdata_xlsx_writer).
-author("yqfclid").


-record(xlsx_write_context,{outfile = "",author="",files=[],sheets=[]}).
-record(xlsx_write_sheet,{title,rows=[],dimension}).
-record(xlsx_write_file,{name,content}).

%% API
-export([
    create/2,
    add_sheet/2,
    create_sheet/2,
    close/1]).

%%%===================================================================
%%% API
%%%===================================================================
create(Xlsx,Author)->
    Handle = erlang:system_time(nano_seconds),
    put_context(Handle,#xlsx_write_context{outfile = Xlsx,author = Author}),
    Handle.

add_sheet(XlsxHandle,SheetHandle)->
    Context =#xlsx_write_context{sheets = Sheets}= get_context(XlsxHandle),
    NewContext = Context#xlsx_write_context{sheets = Sheets++ [SheetHandle]},
    put_context(XlsxHandle,NewContext).

create_sheet(Title,Rows)->
    SheetHandle = erlang:system_time(nano_seconds),
    Dimension = normalize_dimension(Rows),
    SheetContext = #xlsx_write_sheet{title=Title,rows = add_rows([],Rows),dimension = Dimension},
    put_sheet(SheetHandle,SheetContext),
    SheetHandle.

add_rows(OldRows,NewRows)->
    lists:foldl(
        fun([Index|Row],Acc)->
            lists:keystore(Index,1,Acc,{Index,Row})
        end,OldRows,NewRows).


put_sheet(SheetHandle,SheetContext)->
    put({xlsx_write_sheet,SheetHandle},SheetContext).

get_sheet(SheetHandle)->
    get({xlsx_write_sheet,SheetHandle}).

normalize_dimension(Rows)->
    {MaxColumn,MaxLine} = lists:foldl(
        fun([LineId|Row],{MaxCol,MaxLine})->
            MC = if length(Row)> MaxCol-> length(Row);
                     true-> MaxCol
                 end,
            ML = if LineId> MaxLine-> LineId;
                     true-> MaxLine
                 end,
            {MC,ML}
        end,{0,0},Rows),
    ["A1:" , alinkdata_xlsx_util:get_field_string(MaxColumn ,MaxLine)].

put_context(XlsxHandle,Context)->
    put({xlsx_write_context,XlsxHandle},Context).
get_context(XlsxHandle)->
    get({xlsx_write_context,XlsxHandle}).

clear_sheet_context(SheetHandle)->
    erase({xlsx_write_sheet,SheetHandle}).
clear_context(XlsxHandle)->
    #xlsx_write_context{sheets = Sheets}= get_context(XlsxHandle),
    lists:foreach(fun clear_sheet_context/1,Sheets),
    erase({xlsx_write_context,XlsxHandle}).

close(XlsxHandle)->
    Ret = process_save(XlsxHandle),
    clear_context(XlsxHandle),
    Ret.

process_save(XlsxHandle)->
    Context = #xlsx_write_context{outfile = File,sheets = Sheets} = get_context(XlsxHandle),
    Files = do_sheets(Sheets),
    Funs =
        [
            fun do_doc_props/2,
            fun do_relation/2,
            fun do_style/2,
            fun do_workbook_relation/2,
            fun do_content_types/2,
            fun do_workbook/2
        ],

    AllFiles = lists:foldl(fun(F,Acc)-> F(Context,Acc) end, Files,Funs),
    ZipList = lists:map(fun(NF)-> {NF#xlsx_write_file.name, list_to_binary(NF#xlsx_write_file.content) } end,AllFiles),
    zip:create(File,ZipList, [memory]).

do_sheets(Sheets)->
    lists:map(
        fun({Idx,SheetHandle}) ->
            I = integer_to_list(Idx),
            #xlsx_write_sheet{dimension = Dimension,rows = Rows} = get_sheet(SheetHandle),
            #xlsx_write_file{
                name = "xl/worksheets/sheet" ++ I ++ ".xml",
                content = [
                    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                    "<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
                    , Dimension
                    ,"<sheetData>" ,
                    lists:map(fun({RIdx,Row})->encode_row(RIdx, Row) end, Rows),
                    "</sheetData></worksheet>"]
            }
        end, lists:zip(lists:seq(1, length(Sheets)), Sheets)).

do_doc_props(#xlsx_write_context{author = Author}, Acc) ->
    {{Y,M,D},{H,Min,S}} = erlang:universaltime(),
    [
        #xlsx_write_file{
            name = "docProps/core.xml",
            content =[
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
                "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
                "   <dc:creator>",Author,"</dc:creator>"
                "   <cp:lastModifiedBy>",Author,"</cp:lastModifiedBy>"
                "   <dcterms:created xsi:type=\"dcterms:W3CDTF\">",io_lib:format("~p-~2..0w-~2..0wT~2..0w:~2..0w:~2..0wZ",[Y,M,D,H,Min,S]),"</dcterms:created>"
                "   <cp:revision>0</cp:revision>"
                "</cp:coreProperties>"]
        },
        #xlsx_write_file{
            name = "docProps/app.xml",
            content =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">"
            "  <TotalTime>0</TotalTime>"
            "</Properties>"
        } | Acc
    ].

do_relation(_Context,Acc)->
    [
        #xlsx_write_file{
            name = "_rels/.rels",
            content = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
            "  <Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>"
            "  <Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>"
            "  <Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>"
            "</Relationships>"
        }
        |Acc
    ].

do_style(_Context,Acc)->
    [
        #xlsx_write_file{
            name = "xl/styles.xml",
            content =

            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<styleSheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">"
            "<numFmts count=\"7\">"
            "<numFmt formatCode=\"GENERAL\" numFmtId=\"164\"/>"
            "<numFmt formatCode=\"&quot;yes&quot;;&quot;yes&quot;;&quot;no&quot;\" numFmtId=\"170\"/>"
            "</numFmts>"
            "<fonts count=\"5\">"
            "  <font><name val=\"Arial\"/><family val=\"2\"/><sz val=\"10\"/></font>"
            "  <font><name val=\"Arial\"/><family val=\"0\"/><sz val=\"10\"/><b val=\"true\"/></font>"
            "  <font><name val=\"Arial\"/><family val=\"0\"/><sz val=\"10\"/><i val=\"true\"/></font>"
            "  <font><name val=\"Arial\"/><family val=\"0\"/><sz val=\"10\"/></font>"
            "  <font><name val=\"Arial\"/><family val=\"2\"/><sz val=\"10\"/></font>"
            "</fonts>"
            "<fills count=\"2\">"
            "  <fill><patternFill patternType=\"none\"/></fill>"
            "  <fill><patternFill patternType=\"gray125\"/></fill>"
            "</fills>"
            "<borders count=\"1\">"
            "  <border diagonalDown=\"false\" diagonalUp=\"false\"><left/><right/><top/><bottom/><diagonal/></border>"
            "</borders>"
            "<cellStyleXfs count=\"20\">"
            "  <xf applyAlignment=\"true\" applyBorder=\"true\" applyFont=\"true\" applyProtection=\"true\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"164\">"
            "    <alignment horizontal=\"general\" indent=\"0\" shrinkToFit=\"false\" textRotation=\"0\" vertical=\"bottom\" wrapText=\"false\"/>"
            "    <protection hidden=\"false\" locked=\"true\"/>"
            "  </xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"2\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"2\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"0\" numFmtId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"43\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"41\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"44\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"42\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"9\"></xf>"
            "  </cellStyleXfs>"
            "<cellXfs count=\"10\">"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"false\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"4\" numFmtId=\"164\" xfId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"4\" numFmtId=\"22\" xfId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"4\" numFmtId=\"15\" xfId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"false\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"4\" numFmtId=\"1\" xfId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"false\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"4\" numFmtId=\"2\" xfId=\"0\"></xf>"
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"4\" numFmtId=\"49\" xfId=\"0\"></xf>"
            %% boolean
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"false\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"4\" numFmtId=\"170\" xfId=\"0\"></xf>"
            %% string bold
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"1\" numFmtId=\"49\" xfId=\"0\"></xf>"
            %% string italic
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"0\" fontId=\"2\" numFmtId=\"49\" xfId=\"0\"></xf>"

            %% string filled
            "  <xf applyAlignment=\"false\" applyBorder=\"false\" applyFont=\"true\" applyProtection=\"false\" borderId=\"0\" fillId=\"1\" fontId=\"0\" numFmtId=\"49\" xfId=\"0\"></xf>"

            "</cellXfs>"
            "<cellStyles count=\"6\"><cellStyle builtinId=\"0\" customBuiltin=\"false\" name=\"Normal\" xfId=\"0\"/>"
            "  <cellStyle builtinId=\"3\" customBuiltin=\"false\" name=\"Comma\" xfId=\"15\"/>"
            "  <cellStyle builtinId=\"6\" customBuiltin=\"false\" name=\"Comma [0]\" xfId=\"16\"/>"
            "  <cellStyle builtinId=\"4\" customBuiltin=\"false\" name=\"Currency\" xfId=\"17\"/>"
            "  <cellStyle builtinId=\"7\" customBuiltin=\"false\" name=\"Currency [0]\" xfId=\"18\"/>"
            "  <cellStyle builtinId=\"5\" customBuiltin=\"false\" name=\"Percent\" xfId=\"19\"/>"
            "</cellStyles>"
            "</styleSheet>"
        }
        |Acc
    ].


do_workbook_relation(#xlsx_write_context{sheets = Sheets},Acc)->
    [
        #xlsx_write_file{
            name = "xl/_rels/workbook.xml.rels",
            content =[
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
                "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
                "<Relationship Id=\"rId0\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/>"
                ,
                lists:map(
                    fun(Id)->
                        I = integer_to_list(Id),
                        ["<Relationship Id=\"sheet" ,  I ,"\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet" ,  I , ".xml\"/>"]
                    end,lists:seq(1,length(Sheets))),
                "<Relationship Id=\"rId99\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" Target=\"xl/sharedStrings.xml\"/>",
                "</Relationships>"
            ]
        }|Acc
    ].

do_content_types(#xlsx_write_context{sheets = Sheets},Acc)->
    [#xlsx_write_file{
        name = "[Content_Types].xml",
        content =
        ["<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
        "<Override PartName=\"/_rels/.rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
        "<Override PartName=\"/docProps/core.xml\" ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>"
        "<Override PartName=\"/docProps/app.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>"
        "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>"
        "<Override PartName=\"/xl/_rels/workbook.xml.rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
        "<Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>"
            ,
            lists:map(
                fun(Id)->
                    I = integer_to_list(Id),
                    ["<Override PartName=\"/xl/worksheets/sheet", I, ".xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>"]
                end,lists:seq(1,length(Sheets)))

            ,"<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>"
        "</Types>"]
    }|Acc]
.


do_workbook(#xlsx_write_context{sheets = Sheets},Acc)->
    [
        #xlsx_write_file{
            name = "xl/workbook.xml",
            content =
            ["<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
            "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\">"
            "<workbookPr date1904=\"0\" />"
            "<sheets>" ,
                lists:map(
                    fun({Idx, SheetHandle}) ->
                        I = integer_to_list(Idx),
                        #xlsx_write_sheet{title = SheetName} = get_sheet(SheetHandle),
                        ["<sheet name=\"" , alinkdata_xlsx_util:escape(SheetName) , "\" sheetId=\"" , I , "\" r:id=\"sheet" ,I , "\"/>"]
                    end,
                    lists:zip(lists:seq(1, length(Sheets)), Sheets)) ,
                "</sheets></workbook>"]
        }|Acc].

encode_row(Idx, Row) ->
    I = integer_to_list(Idx),
    ["<row r=\"", I, "\">",
        [begin
             {Kind, Content, Style} = encode(Cell),
             ["<c r=\"", column(Col), I, "\" t=\"", atom_to_list(Kind), "\"",
                 " s=\"", integer_to_list(Style), "\">", Content, "</c>"
             ]
         end || {Col, Cell} <- lists:zip(lists:seq(0, length(Row)-1), Row)],
        "</row>"].

%% Given 0-based column number return the Excel column name as list.
column(N) ->
    column(N, []).
column(N, Acc) when N < 26 ->
    [(N)+$A | Acc];
column(N, Acc) ->
    column(N div 26-1, [(N rem 26)+$A|Acc]).

%% @doc Encode an Erlang term as an Excel cell.
encode(true) ->
    {b, "<v>1</v>", 6};
encode(false) ->
    {b, "<v>0</v>", 6};
encode(I) when is_integer(I) ->
    {n, ["<v>", integer_to_list(I), "</v>"], 3};
encode(F) when is_float(F) ->
    {n, ["<v>", float_to_list(F), "</v>"], 4};
encode(_Dt = {{_,_,_},{_,_,_}}) ->
    {n, ["<v>111</v>"], 3};
%% @doc bold
encode({b, Str}) ->
    {inlineStr, ["<is><t>", alinkdata_xlsx_util:escape(alinkdata_xlsx_util:to_list(Str)), "</t></is>"], 7};
%% @doc bold
encode({i, Str}) ->
    {inlineStr, ["<is><t>", alinkdata_xlsx_util:escape(alinkdata_xlsx_util:to_list(Str)), "</t></is>"], 8};
encode({f, Str}) ->
    {inlineStr, ["<is><t>", alinkdata_xlsx_util:escape(alinkdata_xlsx_util:to_list(Str)), "</t></is>"], 9};
encode(Str) ->
    {inlineStr, ["<is><t>", alinkdata_xlsx_util:escape(alinkdata_xlsx_util:to_list(Str)), "</t></is>"], 5}.