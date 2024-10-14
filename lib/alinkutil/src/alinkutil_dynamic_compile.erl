%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2022
%%% @doc
%%% Description :
%%% Authors : Mats Cronqvist <mats.cronqvist@ericsson.com>
%%%           Chris Newcombe <chris.newcombe@gmail.com>
%%%           Jacob Vorreuter <jacob.vorreuter@gmail.com>
%%% TODO :
%%% - add support for limit include-file depth (and prevent circular references)
%%%   prevent circular macro expansion set FILE correctly when -module() is found
%%% -include_lib support $ENVVAR in include filenames
%%%  substitute-stringize (??MACRO)
%%% -undef/-ifdef/-ifndef/-else/-endif
%%% -file(File, Line)
%%% @end
%% this is copied from rabbitmq, rabbitmq is a great project
%%% Created : 25. 6月 2022 上午11:21
%%%-------------------------------------------------------------------
-module(alinkutil_dynamic_compile).
-author("yqfclid").

%% API
-export([load_from_string/1, load_from_string/2]).
-export([from_string/1, from_string/2]).

-import(lists, [reverse/1, keyreplace/4]).

%%====================================================================
%% API
%%====================================================================
%%--------------------------------------------------------------------
%% Function:
%% Description:
%%   Compile module from string and load into VM
%%--------------------------------------------------------------------
load_from_string(CodeStr) ->
    load_from_string(CodeStr, []).

load_from_string(CodeStr, CompileFormsOptions) ->
    {Mod, Bin} = from_string(CodeStr, CompileFormsOptions),
    code:load_binary(Mod, [], Bin).

%%--------------------------------------------------------------------
%% Function:
%% Description:
%%   Returns a binary that can be used with
%%           code:load_binary(Module, ModuleFilenameForInternalRecords, Binary).
%%--------------------------------------------------------------------
from_string(CodeStr) ->
    from_string(CodeStr, []).

% takes Options as for compile:forms/2
from_string(CodeStr, CompileFormsOptions) ->
    %% Initialise the macro dictionary with the default predefined macros,
    %% (adapted from epp.erl:predef_macros/1
    Filename = "compiled_from_string",
    %%Machine  = list_to_atom(erlang:system_info(machine)),
    Ms0 = dict:new(),
    % Ms1    = dict:store('FILE',          {[], "compiled_from_string"}, Ms0),
    % Ms2    = dict:store('LINE',          {[], 1}, Ms1),  % actually we might add special code for this
    % Ms3    = dict:store('MODULE',        {[], undefined},              Ms2),
    % Ms4    = dict:store('MODULE_STRING', {[], undefined},              Ms3),
    % Ms5    = dict:store('MACHINE',       {[], Machine},                Ms4),
    % InitMD = dict:store(Machine,         {[], true},                   Ms5),
    InitMD = Ms0,

    %% From the docs for compile:forms:
    %%    When encountering an -include or -include_dir directive, the compiler searches for header files in the following directories:
    %%      1. ".", the current working directory of the file server;
    %%      2. the base name of the compiled file;
    %%      3. the directories specified using the i option. The directory specified last is searched first.
    %% In this case, #2 is meaningless.
    IncludeSearchPath = ["." | reverse([Dir || {i, Dir} <- CompileFormsOptions])],
    {RevForms, _OutMacroDict} = scan_and_parse(CodeStr, Filename, 1, [], InitMD, IncludeSearchPath),
    Forms = [{attribute, 0, file, {"compiled_from_string", 0}} | reverse([{eof, 0} | RevForms])],

    %% note: 'binary' is forced as an implicit option, whether it is provided or not.
    case compile:forms(Forms, CompileFormsOptions) of
        {ok, ModuleName, CompiledCodeBinary} when is_binary(CompiledCodeBinary) ->
            {ModuleName, CompiledCodeBinary};
        {ok, ModuleName, CompiledCodeBinary, []} when is_binary(CompiledCodeBinary) ->  % empty warnings list
            {ModuleName, CompiledCodeBinary};
        {ok, _ModuleName, _CompiledCodeBinary, Warnings} ->
            throw({?MODULE, warnings, Warnings});
        Other ->
            throw({?MODULE, compile_forms, Other})
    end.

%%====================================================================
%% Internal functions
%%====================================================================
%%% Code from Mats Cronqvist
%%% See http://www.erlang.org/pipermail/erlang-questions/2007-March/025507.html
%%%## 'scan_and_parse'
%%%
%%% basically we call the OTP scanner and parser (erl_scan and
%%% erl_parse) line-by-line, but check each scanned line for (or
%%% definitions of) macros before parsing.
%% returns {ReverseForms, FinalMacroDict}
scan_and_parse([], _CurrFilename, _CurrLine, RevForms, MacroDict, _IncludeSearchPath) ->
    {RevForms, MacroDict};

scan_and_parse(RemainingText, CurrFilename, CurrLine, RevForms, MacroDict, IncludeSearchPath) ->
    case scanner(RemainingText, CurrLine, MacroDict) of
        {tokens, NLine, NRemainingText, Toks} ->
            {ok, Form0} = erl_parse:parse_form(Toks),
            {Form, Forms} = normalize_record(Form0),
            scan_and_parse(NRemainingText, CurrFilename, NLine, [Form | Forms ++ RevForms], MacroDict, IncludeSearchPath);
        {macro, NLine, NRemainingText, NMacroDict} ->
            scan_and_parse(NRemainingText, CurrFilename, NLine, RevForms, NMacroDict, IncludeSearchPath);
        {include, NLine, NRemainingText, IncludeFilename} ->
            IncludeFileRemainingTextents = read_include_file(IncludeFilename, IncludeSearchPath),
            %%io:format("include file ~p contents: ~n~p~nRemainingText = ~p~n", [IncludeFilename,IncludeFileRemainingTextents, RemainingText]),
            %% Modify the FILE macro to reflect the filename
            %%IncludeMacroDict = dict:store('FILE', {[],IncludeFilename}, MacroDict),
            IncludeMacroDict = MacroDict,

            %% Process the header file (inc. any nested header files)
            {RevIncludeForms, IncludedMacroDict} = scan_and_parse(IncludeFileRemainingTextents, IncludeFilename, 1, [], IncludeMacroDict, IncludeSearchPath),
            %io:format("include file results = ~p~n", [R]),
            %% Restore the FILE macro in the NEW MacroDict (so we keep any macros defined in the header file)
            %%NMacroDict = dict:store('FILE', {[],CurrFilename}, IncludedMacroDict),
            NMacroDict = IncludedMacroDict,

            %% Continue with the original file
            scan_and_parse(NRemainingText, CurrFilename, NLine, RevIncludeForms ++ RevForms, NMacroDict, IncludeSearchPath);
        done ->
            scan_and_parse([], CurrFilename, CurrLine, RevForms, MacroDict, IncludeSearchPath)
    end.

scanner(Text, Line, MacroDict) ->
    case erl_scan:tokens([], Text, Line) of
        {done, {ok, Toks, NLine}, LeftOverChars} ->
            case pre_proc(Toks, MacroDict) of
                {tokens, NToks} -> {tokens, NLine, LeftOverChars, NToks};
                {macro, NMacroDict} -> {macro, NLine, LeftOverChars, NMacroDict};
                {include, Filename} -> {include, NLine, LeftOverChars, Filename}
            end;
        {more, _Continuation} ->
            %% This is supposed to mean "term is not yet complete" (i.e. a '.' has
            %% not been reached yet).
            %% However, for some bizarre reason we also get this if there is a comment after the final '.' in a file.
            %% So we check to see if Text only consists of comments.
            case is_only_comments(Text) of
                true ->
                    done;
                false ->
                    throw({incomplete_term, Text, Line})
            end
    end.

is_only_comments(Text) -> is_only_comments(Text, not_in_comment).

is_only_comments([], _) -> true;
is_only_comments([$  | T], not_in_comment) ->
    is_only_comments(T, not_in_comment); % skipping whitspace outside of comment
is_only_comments([$\t | T], not_in_comment) ->
    is_only_comments(T, not_in_comment); % skipping whitspace outside of comment
is_only_comments([$\n | T], not_in_comment) ->
    is_only_comments(T, not_in_comment); % skipping whitspace outside of comment
is_only_comments([$% | T], not_in_comment) -> is_only_comments(T, in_comment);     % found start of a comment
is_only_comments(_, not_in_comment) -> false;
% found any significant char NOT in a comment
is_only_comments([$\n | T], in_comment) -> is_only_comments(T, not_in_comment); % found end of a comment
is_only_comments([_ | T], in_comment) -> is_only_comments(T, in_comment).     % skipping over in-comment chars

%%%## 'pre-proc'
%%%
%%% have to implement a subset of the pre-processor, since epp insists
%%% on running on a file.
%%% only handles 2 cases;
%% -define(MACRO, something).
%% -define(MACRO(VAR1,VARN),{stuff,VAR1,more,stuff,VARN,extra,stuff}).
pre_proc([{'-', _}, {atom, _, define}, {'(', _}, {_, _, Name} | DefToks], MacroDict) ->
    false = dict:is_key(Name, MacroDict),
    case DefToks of
        [{',', _} | Macro] ->
            {macro, dict:store(Name, {[], macro_body_def(Macro, [])}, MacroDict)};
        [{'(', _} | Macro] ->
            {macro, dict:store(Name, macro_params_body_def(Macro, []), MacroDict)}
    end;

pre_proc([{'-', _}, {atom, _, include}, {'(', _}, {string, _, Filename}, {')', _}, {dot, _}], _MacroDict) ->
    {include, Filename};

pre_proc(Toks, MacroDict) ->
    {tokens, subst_macros(Toks, MacroDict)}.

macro_params_body_def([{')', _}, {',', _} | Toks], RevParams) ->
    {reverse(RevParams), macro_body_def(Toks, [])};
macro_params_body_def([{var, _, Param} | Toks], RevParams) ->
    macro_params_body_def(Toks, [Param | RevParams]);
macro_params_body_def([{',', _}, {var, _, Param} | Toks], RevParams) ->
    macro_params_body_def(Toks, [Param | RevParams]).

macro_body_def([{')', _}, {dot, _}], RevMacroBodyToks) ->
    reverse(RevMacroBodyToks);
macro_body_def([Tok | Toks], RevMacroBodyToks) ->
    macro_body_def(Toks, [Tok | RevMacroBodyToks]).

subst_macros(Toks, MacroDict) ->
    reverse(subst_macros_rev(Toks, MacroDict, [])).

%% returns a reversed list of tokes
subst_macros_rev([{'?', _}, {_, LineNum, 'LINE'} | Toks], MacroDict, RevOutToks) ->
    %% special-case for ?LINE, to avoid creating a new MacroDict for every line in the source file
    subst_macros_rev(Toks, MacroDict, [{integer, LineNum, LineNum}] ++ RevOutToks);

subst_macros_rev([{'?', _}, {_, _, Name}, {'(', _} = Paren | Toks], MacroDict, RevOutToks) ->
    case dict:fetch(Name, MacroDict) of
        {[], MacroValue} ->
            %% This macro does not have any vars, so ignore the fact that the invocation is followed by "(...stuff"
            %% Recursively expand any macro calls inside this macro's value
            %% TODO: avoid infinite expansion due to circular references (even indirect ones)
            RevExpandedOtherMacrosToks = subst_macros_rev(MacroValue, MacroDict, []),
            subst_macros_rev([Paren | Toks], MacroDict, RevExpandedOtherMacrosToks ++ RevOutToks);
        ParamsAndBody ->
            %% This macro does have vars.
            %% Collect all of the passe arguments, in an ordered list
            {NToks, Arguments} = subst_macros_get_args(Toks, []),
            %% Expand the varibles
            ExpandedParamsToks = subst_macros_subst_args_for_vars(ParamsAndBody, Arguments),
            %% Recursively expand any macro calls inside this macro's value
            %% TODO: avoid infinite expansion due to circular references (even indirect ones)
            RevExpandedOtherMacrosToks = subst_macros_rev(ExpandedParamsToks, MacroDict, []),
            subst_macros_rev(NToks, MacroDict, RevExpandedOtherMacrosToks ++ RevOutToks)
    end;

subst_macros_rev([{'?', _}, {_, _, Name} | Toks], MacroDict, RevOutToks) ->
    %% This macro invocation does not have arguments.
    %% Therefore the definition should not have parameters
    {[], MacroValue} = dict:fetch(Name, MacroDict),

    %% Recursively expand any macro calls inside this macro's value
    %% TODO: avoid infinite expansion due to circular references (even indirect ones)
    RevExpandedOtherMacrosToks = subst_macros_rev(MacroValue, MacroDict, []),
    subst_macros_rev(Toks, MacroDict, RevExpandedOtherMacrosToks ++ RevOutToks);

subst_macros_rev([Tok | Toks], MacroDict, RevOutToks) ->
    subst_macros_rev(Toks, MacroDict, [Tok | RevOutToks]);
subst_macros_rev([], _MacroDict, RevOutToks) -> RevOutToks.

subst_macros_get_args([{')', _} | Toks], RevArgs) ->
    {Toks, reverse(RevArgs)};
subst_macros_get_args([{',', _}, {var, _, ArgName} | Toks], RevArgs) ->
    subst_macros_get_args(Toks, [ArgName | RevArgs]);
subst_macros_get_args([{var, _, ArgName} | Toks], RevArgs) ->
    subst_macros_get_args(Toks, [ArgName | RevArgs]).

subst_macros_subst_args_for_vars({[], BodyToks}, []) ->
    BodyToks;
subst_macros_subst_args_for_vars({[Param | Params], BodyToks}, [Arg | Args]) ->
    NBodyToks = keyreplace(Param, 3, BodyToks, {var, 1, Arg}),
    subst_macros_subst_args_for_vars({Params, NBodyToks}, Args).

read_include_file(Filename, IncludeSearchPath) ->
    case file:path_open(IncludeSearchPath, Filename, [read, raw, binary]) of
        {ok, IoDevice, FullName} ->
            {ok, Data} = file:read(IoDevice, filelib:file_size(FullName)),
            file:close(IoDevice),
            binary_to_list(Data);
        {error, Reason} ->
            throw({failed_to_read_include_file, Reason, Filename, IncludeSearchPath})
    end.

normalize_record({attribute, La, record, {Record, Fields}} = Form) ->
    case epp:normalize_typed_record_fields(Fields) of
        {typed, NewFields} ->
            {{attribute, La, record, {Record, NewFields}},
                [{attribute, La, type,
                    {{record, Record}, Fields, []}}]};
        not_typed ->
            {Form, []}
    end;
normalize_record(Form) ->
    {Form, []}.
