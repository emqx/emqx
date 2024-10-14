%%%-------------------------------------------------------------------
%%% @author yqfclid
%%% @copyright (C) 2023, yuqinfeng17@gmail.com
%%% @doc
%%%
%%% @end
%%% Created : 29. 6月 2023 下午1:45
%%%-------------------------------------------------------------------
-module(alinkdata_xlsx_util).
-include_lib("xmerl/include/xmerl.hrl").
%% API
-compile(export_all).
-define(MIN_EXP, -1074).
-define(FLOAT_BIAS, 1022).
-define(BIG_POW, 4503599627370496).

sprintf(Format,Args)->
    lists:flatten(io_lib:format(Format,Args)).

is_record(Term, RecordTag) when is_tuple(Term)  ->
    element(1, Term) =:= RecordTag;
is_record(_Term, _RecordTag)-> false.



has_attribute_value( AttrName, AttValue,XmlNode) ->
    lists:any(fun(Attr) -> (Attr#xmlAttribute.name =:= AttrName) and
        (Attr#xmlAttribute.value =:= AttValue)
              end, XmlNode#xmlElement.attributes).

xmlattribute_value_from_name(Name, XmlNode) ->
    case lists:keyfind(Name,#xmlAttribute.name,XmlNode#xmlElement.attributes) of
        false-> error(sprintf("xmlattribute_value_from_name exception: ~p ~p",[Name,XmlNode]));
        Attribute-> Attribute#xmlAttribute.value
    end.

xmlElement_from_name(Name,XmlNode)->
    case lists:keyfind(Name,#xmlElement.name,XmlNode) of
        false-> error(sprintf("xmlElement_from_name exception: ~p ~p",[Name,XmlNode]));
        Element-> Element
    end.



get_column_count(DimString) ->
    [_BeginStr, EndStr] = case string:tokens(DimString, ":") of
                              [BeginString, EndString] -> [BeginString, EndString];
                              [BeginString] -> [BeginString, BeginString]
                          end,
    [End] = string:tokens(EndStr, "0123456789"),
    field_to_num(End).


-define(APHALIC_LENGTH, ($Z - $A + 1) ).

get_column_string(Column)->
    lists:reverse(do_get_column_string(Column)).

do_get_column_string(Column)when Column < ?APHALIC_LENGTH  ->
    [$A + Column ];
do_get_column_string(Column)->
    [ $A  + Column rem ?APHALIC_LENGTH  | do_get_column_string(( Column div ?APHALIC_LENGTH) -1)].

int_pow(_Base, 0) ->
    1;
int_pow(Base, Exp) when Exp> 0 ->
    int_pow(Base, Exp, 1).


int_pow(Base, Exp, R) when Exp < 2 ->
    R * Base;
int_pow(Base, Exp, R) ->
    int_pow(Base * Base, Exp bsr 1, case Exp band 1 of 1 -> R * Base; 0 -> R end).

field_to_num(NumStr) ->
    {_, Num} = lists:foldr(
        fun(C, {I, N}) ->
            New = (C - $A + 1) * int_pow( $Z - $A + 1,I) + N,
            {I + 1, New}
        end, {0, 0}, NumStr),
    Num.

get_field_number(FieldString)->
    [FieldName] = string:tokens(FieldString, "0123456789"),
    PostNumString = string:right(FieldString, length(FieldString) - length(FieldName)),
    {field_to_num(FieldName), list_to_integer(PostNumString)}.

%% @spec int_ceil(F::float()) -> integer()
%% @doc  Return the ceiling of F as an integer. The ceiling is defined as
%%       F when F == trunc(F);
%%       trunc(F) when F &lt; 0;
%%       trunc(F) + 1 when F &gt; 0.
int_ceil(X) ->
    T = trunc(X),
    case (X - T) of
        Pos when Pos > 0 -> T + 1;
        _ -> T
    end.

get_field_string(Column,Row)->
    [get_column_string(Column) , integer_to_list(Row)].

take_nth_list(N, List, Value)->
    {NewList, _Acc} = lists:mapfoldl(
        fun(Item, Index)->
            if Index =:= N->
                {Value, Index + 1};
                true->
                    {Item, Index + 1}
            end
        end, 1, List),
    NewList.

str_normalize(String)->
    New = replace(String,"_x000D_","\n"),
    replace(New,"_x000d_","\n").

token_str(String, TokenStr)->
    Len = string:len(TokenStr),
    case string:str(String, TokenStr) of
        0->[String];
        1->
            LeftString = string:sub_string(String, Len + 1),
            token_str(LeftString, TokenStr);
        I->
            FirstString = string:sub_string(String, 1, I - 1),
            LeftString = string:sub_string(String, I + Len),
            [FirstString | token_str(LeftString, TokenStr)]
    end.

replace(String, TokenStr, NewString)->
    SplitStrings = token_str(String, TokenStr),
    string:join(SplitStrings, NewString).

%% @doc Convert (almost) any value to a list.
-spec to_list(term()) -> string().
to_list(undefined) -> [];
to_list(<<>>) -> [];
to_list({rsc_list, L}) -> L;
to_list(L) when is_list(L) -> L;
to_list(A) when is_atom(A) -> atom_to_list(A);
to_list(B) when is_binary(B) -> binary_to_list(B);
to_list(I) when is_integer(I) -> integer_to_list(I);
to_list(F) when is_float(F) -> digits(F).

unpack(Float) ->
    <<Sign:1, Exp:11, Frac:52>> = <<Float:64/float>>,
    {Sign, Exp, Frac}.


frexp_int(F) ->
    case unpack(F) of
        {_Sign, 0, Frac} ->
            {Frac, ?MIN_EXP};
        {_Sign, Exp, Frac} ->
            {Frac + (1 bsl 52), Exp - 53 - ?FLOAT_BIAS}
    end.

scale(R, S, MPlus, MMinus, LowOk, HighOk, Float) ->
    Est = xlsx_util:int_ceil(math:log10(abs(Float)) - 1.0e-10),
    %% Note that the scheme implementation uses a 326 element look-up table
    %% for int_pow(10, N) where we do not.
    case Est >= 0 of
        true ->
            fixup(R, S * xlsx_util:int_pow(10, Est), MPlus, MMinus, Est,
                LowOk, HighOk);
        false ->
            Scale = xlsx_util:int_pow(10, -Est),
            fixup(R * Scale, S, MPlus * Scale, MMinus * Scale, Est,
                LowOk, HighOk)
    end.

fixup(R, S, MPlus, MMinus, K, LowOk, HighOk) ->
    TooLow = case HighOk of
                 true ->
                     (R + MPlus) >= S;
                 false ->
                     (R + MPlus) > S
             end,
    case TooLow of
        true ->
            [(K + 1) | generate(R, S, MPlus, MMinus, LowOk, HighOk)];
        false ->
            [K | generate(R * 10, S, MPlus * 10, MMinus * 10, LowOk, HighOk)]
    end.

generate(R0, S, MPlus, MMinus, LowOk, HighOk) ->
    D = R0 div S,
    R = R0 rem S,
    TC1 = case LowOk of
              true ->
                  R =< MMinus;
              false ->
                  R < MMinus
          end,
    TC2 = case HighOk of
              true ->
                  (R + MPlus) >= S;
              false ->
                  (R + MPlus) > S
          end,
    case TC1 of
        false ->
            case TC2 of
                false ->
                    [D | generate(R * 10, S, MPlus * 10, MMinus * 10,
                        LowOk, HighOk)];
                true ->
                    [D + 1]
            end;
        true ->
            case TC2 of
                false ->
                    [D];
                true ->
                    case R * 2 < S of
                        true ->
                            [D];
                        false ->
                            [D + 1]
                    end
            end
    end.

digits1(Float, Exp, Frac) ->
    Round = ((Frac band 1) =:= 0),
    case Exp >= 0 of
        true ->
            BExp = 1 bsl Exp,
            case (Frac =/= ?BIG_POW) of
                true ->
                    scale((Frac * BExp * 2), 2, BExp, BExp,
                        Round, Round, Float);
                false ->
                    scale((Frac * BExp * 4), 4, (BExp * 2), BExp,
                        Round, Round, Float)
            end;
        false ->
            case (Exp =:= ?MIN_EXP) orelse (Frac =/= ?BIG_POW) of
                true ->
                    scale((Frac * 2), 1 bsl (1 - Exp), 1, 1,
                        Round, Round, Float);
                false ->
                    scale((Frac * 4), 1 bsl (2 - Exp), 2, 1,
                        Round, Round, Float)
            end
    end.



digits(N) when is_integer(N) ->
    integer_to_list(N);
digits(0.0) ->
    "0.0";
digits(Float) ->
    {Frac1, Exp1} = frexp_int(Float),
    [Place0 | Digits0] = digits1(Float, Exp1, Frac1),
    {Place, Digits} = transform_digits(Place0, Digits0),
    R = insert_decimal(Place, Digits),
    case Float < 0 of
        true ->
            [$- | R];
        _ ->
            R
    end.

transform_digits(Place, [0 | Rest]) ->
    transform_digits(Place, Rest);
transform_digits(Place, Digits) ->
    {Place, [$0 + D || D <- Digits]}.



insert_decimal(0, S) ->
    "0." ++ S;
insert_decimal(Place, S) when Place > 0 ->
    L = length(S),
    case Place - L of
        0 ->
            S ++ ".0";
        N when N < 0 ->
            {S0, S1} = lists:split(L + N, S),
            S0 ++ "." ++ S1;
        N when N < 6 ->
            %% More places than digits
            S ++ lists:duplicate(N, $0) ++ ".0";
        _ ->
            insert_decimal_exp(Place, S)
    end;
insert_decimal(Place, S) when Place > -6 ->
    "0." ++ lists:duplicate(abs(Place), $0) ++ S;
insert_decimal(Place, S) ->
    insert_decimal_exp(Place, S).

insert_decimal_exp(Place, S) ->
    [C | S0] = S,
    S1 = case S0 of
             [] ->
                 "0";
             _ ->
                 S0
         end,
    Exp = case Place < 0 of
              true ->
                  "e-";
              false ->
                  "e+"
          end,
    [C] ++ "." ++ S1 ++ Exp ++ integer_to_list(abs(Place - 1)).



%% @doc Escape a string so that it is valid within HTML/ XML.
%% @spec escape(iolist()) -> binary()
-spec escape(list()|binary()|{trans, list()}) -> binary() | undefined.
escape({trans, Tr}) ->
    {trans, [{Lang, escape(V)} || {Lang,V} <- Tr]};
escape(undefined) ->
    undefined;
escape(<<>>) ->
    <<>>;
escape([]) ->
    <<>>;
escape(L) when is_list(L) ->
    escape(list_to_binary(L));
escape(B) when is_binary(B) ->
    escape1(B, <<>>).

escape1(<<>>, Acc) ->
    Acc;
escape1(<<"&euro;", T/binary>>, Acc) ->
    escape1(T, <<Acc/binary, "€">>);
escape1(<<$&, T/binary>>, Acc) ->
    escape1(T, <<Acc/binary, "&amp;">>);
escape1(<<$<, T/binary>>, Acc) ->
    escape1(T, <<Acc/binary, "&lt;">>);
escape1(<<$>, T/binary>>, Acc) ->
    escape1(T, <<Acc/binary, "&gt;">>);
escape1(<<$", T/binary>>, Acc) ->
    escape1(T, <<Acc/binary, "&quot;">>);
escape1(<<$', T/binary>>, Acc) ->
    escape1(T, <<Acc/binary, "&#39;">>);
escape1(<<C, T/binary>>, Acc) ->
    escape1(T, <<Acc/binary, C>>).