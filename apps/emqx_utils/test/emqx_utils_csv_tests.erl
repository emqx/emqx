%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_utils_csv_tests).

-import(emqx_utils_csv, [escape_field/1]).

-include_lib("eunit/include/eunit.hrl").

%% Test cases for binary input
binary_escape_test_() ->
    [
        %% No special characters - should return as binary
        ?_assertEqual(<<"simple">>, escape_field(<<"simple">>)),
        ?_assertEqual(<<"client123">>, escape_field(<<"client123">>)),
        ?_assertEqual(<<"test_client">>, escape_field(<<"test_client">>)),

        %% Contains comma - should be escaped (returns binary when binary input has special chars)
        ?_assertEqual(<<"\"client,with,comma\"">>, escape_field(<<"client,with,comma">>)),
        ?_assertEqual(<<"\"a,b,c\"">>, escape_field(<<"a,b,c">>)),

        %% Contains quote - should be escaped and quotes doubled (returns binary when binary input has special chars)
        ?_assertEqual(<<"\"client\"\"with\"\"quote\"">>, escape_field(<<"client\"with\"quote">>)),
        ?_assertEqual(<<"\"test\"\"quoted\"\"field\"">>, escape_field(<<"test\"quoted\"field">>)),

        %% Contains newline - should be escaped (returns binary when binary input has special chars)
        ?_assertEqual(<<"\"client\nwith\nnewline\"">>, escape_field(<<"client\nwith\nnewline">>)),
        ?_assertEqual(<<"\"line1\r\nline2\"">>, escape_field(<<"line1\r\nline2">>)),

        %% Contains carriage return - should be escaped (returns binary when binary input has special chars)
        ?_assertEqual(<<"\"client\rwith\rreturn\"">>, escape_field(<<"client\rwith\rreturn">>)),

        %% Contains multiple special characters (returns binary when binary input has special chars)
        ?_assertEqual(
            <<"\"client,\"\"with\"\",\nall\"">>, escape_field(<<"client,\"with\",\nall">>)
        ),
        ?_assertEqual(<<"\"a,b\"\"c\nd\"">>, escape_field(<<"a,b\"c\nd">>)),

        %% Empty binary
        ?_assertEqual(<<>>, escape_field(<<>>)),

        %% Binary with only special characters (returns binary when binary input has special chars)
        ?_assertEqual(<<"\",\"\"\n\r\"">>, escape_field(<<",\"\n\r">>))
    ].

%% Test cases for list input
list_escape_test_() ->
    [
        %% No special characters - should return as list
        ?_assertEqual("simple", escape_field("simple")),
        ?_assertEqual("client123", escape_field("client123")),
        ?_assertEqual("test_client", escape_field("test_client")),

        %% Contains comma - should be escaped
        ?_assertEqual("\"client,with,comma\"", escape_field("client,with,comma")),
        ?_assertEqual("\"a,b,c\"", escape_field("a,b,c")),

        %% Contains quote - should be escaped and quotes doubled
        ?_assertEqual("\"client\"\"with\"\"quote\"", escape_field("client\"with\"quote")),
        ?_assertEqual("\"test\"\"quoted\"\"field\"", escape_field("test\"quoted\"field")),

        %% Contains newline - should be escaped
        ?_assertEqual("\"client\nwith\nnewline\"", escape_field("client\nwith\nnewline")),
        ?_assertEqual("\"line1\r\nline2\"", escape_field("line1\r\nline2")),

        %% Contains carriage return - should be escaped
        ?_assertEqual("\"client\rwith\rreturn\"", escape_field("client\rwith\rreturn")),

        %% Contains multiple special characters
        ?_assertEqual("\"client,\"\"with\"\",\nall\"", escape_field("client,\"with\",\nall")),
        ?_assertEqual("\"a,b\"\"c\nd\"", escape_field("a,b\"c\nd")),

        %% Empty list
        ?_assertEqual([], escape_field([])),

        %% List with only special characters
        ?_assertEqual("\",\"\"\n\r\"", escape_field(",\"\n\r"))
    ].

%% Test cases for edge cases and boundary conditions
edge_cases_test_() ->
    [
        %% Single character tests
        ?_assertEqual("a", escape_field("a")),
        ?_assertEqual("\",\"", escape_field(",")),
        ?_assertEqual("\"\"\"\"", escape_field("\"")),
        ?_assertEqual("\"\n\"", escape_field("\n")),
        ?_assertEqual("\"\r\"", escape_field("\r")),

        %% Unicode characters (should work with UTF-8)
        ?_assertEqual("测试", escape_field("测试")),
        ?_assertEqual(<<"测试"/utf8>>, escape_field(<<"测试"/utf8>>)),
        ?_assertEqual("\"测试,with,comma\"", escape_field("测试,with,comma")),
        ?_assertEqual("café", escape_field("café")),
        ?_assertEqual("\"café\"\"with\"\"quote\"", escape_field("café\"with\"quote")),

        %% Very long strings
        ?_assertEqual(
            "very_long_string_without_special_chars",
            escape_field("very_long_string_without_special_chars")
        ),
        ?_assertEqual(
            "\"very,long,string,with,commas\"",
            escape_field("very,long,string,with,commas")
        ),

        %% Mixed case and numbers
        ?_assertEqual("Client123_Test", escape_field("Client123_Test")),
        ?_assertEqual("\"Client123,Test\"", escape_field("Client123,Test")),

        %% Only spaces (should not be escaped)
        ?_assertEqual("   ", escape_field("   ")),
        ?_assertEqual("\"   ,\"", escape_field("   ,")),

        %% Tabs and other whitespace (should not be escaped unless newline/carriage return)
        ?_assertEqual("\t", escape_field("\t")),
        ?_assertEqual("a\tb", escape_field("a\tb"))
    ].

%% Test cases for CSV compliance (RFC 4180)
csv_compliance_test_() ->
    [
        %% Test that escaped fields can be properly parsed back
        ?_test(begin
            Field = "test,field\"with\"quotes\nand\nnewlines",
            Escaped = escape_field(Field),
            %% Should start and end with quotes
            ?assertEqual($", hd(Escaped)),
            ?assertEqual($", lists:last(Escaped)),
            %% Should have doubled quotes inside
            ?assert(lists:any(fun(C) -> C =:= $" end, Escaped))
        end),

        %% Test that simple fields are not unnecessarily quoted
        ?_test(begin
            SimpleField = "simple_field",
            Escaped = escape_field(SimpleField),
            ?assertEqual(SimpleField, Escaped)
        end),

        %% Test that all special characters are properly handled
        ?_test(begin
            SpecialChars = ",\"\n\r",
            Escaped = escape_field(SpecialChars),
            %% Should be quoted
            ?assertEqual($", hd(Escaped)),
            ?assertEqual($", lists:last(Escaped)),
            %% Should contain the original characters (not escaped)
            ?assert(lists:member($,, Escaped)),
            ?assert(lists:member($\n, Escaped)),
            ?assert(lists:member($\r, Escaped))
        end)
    ].
