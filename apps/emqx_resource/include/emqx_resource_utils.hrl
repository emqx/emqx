%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(SAFE_CALL(_EXP_),
    ?SAFE_CALL(_EXP_, ok)
).

-define(SAFE_CALL(_EXP_, _EXP_ON_FAIL_),
    fun() ->
        try
            (_EXP_)
        catch
            _EXCLASS_:_EXCPTION_:_ST_ ->
                _EXP_ON_FAIL_,
                {error, {_EXCLASS_, _EXCPTION_, _ST_}}
        end
    end()
).
