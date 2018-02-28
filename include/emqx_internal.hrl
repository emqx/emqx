%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

%% Internal Header File

-define(GPROC_POOL(JoinOrLeave, Pool, Id),
        (begin
            case JoinOrLeave of
                join  -> gproc_pool:connect_worker(Pool, {Pool, Id});
                leave -> gproc_pool:disconnect_worker(Pool, {Pool, Id})
            end
        end)).

-define(PROC_NAME(M, I), (list_to_atom(lists:concat([M, "_", I])))).

-define(UNEXPECTED_REQ(Req, State),
        (begin
            lager:error("[~s] Unexpected Request: ~p", [?MODULE, Req]),
            {reply, {error, unexpected_request}, State}
        end)).

-define(UNEXPECTED_MSG(Msg, State),
        (begin
            lager:error("[~s] Unexpected Message: ~p", [?MODULE, Msg]),
            {noreply, State}
        end)).

-define(UNEXPECTED_INFO(Info, State),
        (begin
            lager:error("[~s] Unexpected Info: ~p", [?MODULE, Info]),
            {noreply, State}
        end)).

-define(IF(Cond, TrueFun, FalseFun),
        (case (Cond) of
            true -> (TrueFun);
            false-> (FalseFun)
        end)).

-define(FULLSWEEP_OPTS, [{fullsweep_after, 10}]).

