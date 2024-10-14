%%%-------------------------------------------------------------------
%%% @author kenneth
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 26. 四月 2020 16:35
%%%-------------------------------------------------------------------
-author("kenneth").



-define(DEFAULT, default).
-define(MAX_PROFILE, 100).
-define(PRE, <<"alinkiot_">>).
-define(Table(Name), <<?PRE/binary, Name/binary>>).
-define(Struct(Field, Field1), <<"struct_", Field/binary, "_", Field1/binary>>).

-define(TYPES, [
    <<"TIMESTAMP">>,
    <<"INT">>,
    <<"BIGINT">>,
    <<"FLOAT">>,
    <<"DOUBLE">>,
    <<"BINARY">>,
    <<"SMALLINT">>,
    <<"TINYINT">>,
    <<"BOOL">>,
    <<"NCHAR">>
]).
