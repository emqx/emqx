%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_utils_redact_tests).

-include_lib("eunit/include/eunit.hrl").

is_redacted_test_() ->
    [
        ?_assertNot(is_redacted(password, <<"secretpass">>)),
        ?_assertNot(is_redacted(password, <<>>)),
        ?_assertNot(is_redacted(password, undefined)),
        ?_assert(is_redacted(password, <<"******">>)),
        ?_assertNot(is_redacted(password, fun() -> <<"secretpass">> end)),
        ?_assertNot(is_redacted(password, emqx_secret:wrap(<<"secretpass">>))),
        ?_assert(is_redacted(password, fun() -> <<"******">> end)),
        ?_assert(is_redacted(password, emqx_secret:wrap(<<"******">>)))
    ].

no_redact_template_var_test() ->
    ?assertEqual(
        #{
            password => <<"${var}">>,
            account_key => <<"${path.to.var}">>,
            <<"secret">> => <<"******">>,
            private_key => <<"******">>
        },
        redact(#{
            password => <<"${var}">>,
            <<"secret">> => <<"abc">>,
            account_key => <<"${path.to.var}">>,
            private_key => <<"${var}more">>
        })
    ).

redact(X) -> emqx_utils:redact(X).

is_redacted(Key, Value) ->
    emqx_utils:is_redacted(Key, Value).
