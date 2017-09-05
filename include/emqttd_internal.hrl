%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-define(record_to_proplist(Def, Rec),
        lists:zip(record_info(fields, Def), tl(tuple_to_list(Rec)))).

-define(record_to_proplist(Def, Rec, Fields),
    [{K, V} || {K, V} <- ?record_to_proplist(Def, Rec),
                         lists:member(K, Fields)]).

-define(UNEXPECTED_REQ(Req, State),
        (begin
            lager:error("Unexpected Request: ~p", [Req]),
            {reply, {error, unexpected_request}, State}
        end)).

-define(UNEXPECTED_MSG(Msg, State),
        (begin
            lager:error("Unexpected Message: ~p", [Msg]),
            {noreply, State}
        end)).

-define(UNEXPECTED_INFO(Info, State),
        (begin
            lager:error("Unexpected Info: ~p", [Info]),
            {noreply, State}
        end)).

-define(IF(Cond, TrueFun, FalseFun),
        (case (Cond) of
            true -> (TrueFun);
            false-> (FalseFun)
        end)).

-define(FULLSWEEP_OPTS, [{fullsweep_after, 10}]).

-define(SUCCESS, 0).  %% Success
-define(ERROR1, 101). %% badrpc
-define(ERROR2, 102). %% Unknown error
-define(ERROR3, 103). %% Username or password error
-define(ERROR4, 104). %% Empty username or password
-define(ERROR5, 105). %% User does not exist
-define(ERROR6, 106). %% Admin can not be deleted
-define(ERROR7, 107). %% Missing request parameter
-define(ERROR8, 108). %% Request parameter type error
-define(ERROR9, 109). %% Request parameter is not a json
-define(ERROR10, 110). %% Plugin has been loaded
-define(ERROR11, 111). %% Plugin has been loaded
-define(ERROR12, 112). %% Client not online
-define(ERROR13, 113). %% User already exist
-define(ERROR14, 114). %% OldPassword error

