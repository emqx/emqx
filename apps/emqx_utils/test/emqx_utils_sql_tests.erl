%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_utils_sql_tests).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Properties
%%------------------------------------------------------------------------------

snowflake_escape_test_() ->
    Props = [prop_snowflake_escape()],
    Opts = [{numtests, 1_000}, {to_file, user}, {max_size, 100}],
    {timeout, 300, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.

prop_snowflake_escape() ->
    ?FORALL(
        Input,
        binary(),
        begin
            Escaped = iolist_to_binary(emqx_utils_sql:escape_snowflake(Input)),
            Content = binary_part(Escaped, 1, iolist_size(Escaped) - 2),
            %% Should not have any single double quotes after escaping, except for the
            %% leading and trailing ones.
            ?WHENFAIL(
                io:format(
                    user,
                    "Input:\n  ~s\n\nEscaped:\n  ~s\n",
                    [Input, Escaped]
                ),
                nomatch =:= re:run(Content, <<"[^\"]\"[^\"]">>, [{capture, none}])
            )
        end
    ).
