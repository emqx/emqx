%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_test_lib).

-export([ with_env/2
        ]).

with_env([], F) -> F();
with_env([{Key, Val} | Rest], F) ->
    Origin = get_env(Key),
    try
        ok = set_env(Key, Val),
        with_env(Rest, F)
    after
        case Origin of
            undefined -> ok = unset_env(Key);
            _ -> ok = set_env(Key, Origin)
        end
    end.

get_env(Key) -> application:get_env(?APPLICATION, Key).
set_env(Key, Val) -> application:set_env(?APPLICATION, Key, Val).
unset_env(Key) -> application:unset_env(?APPLICATION, Key).


