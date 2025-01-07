%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(prop_rule_maps).

-include_lib("proper/include/proper.hrl").

prop_get_put_single_key() ->
    ?FORALL(
        {Key, Val},
        {term(), term()},
        begin
            Val =:=
                emqx_rule_maps:nested_get(
                    {var, Key},
                    emqx_rule_maps:nested_put({var, Key}, Val, #{})
                )
        end
    ).
