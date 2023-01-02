%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_lwm2m_sup).

-behaviour(supervisor).

-export([ start_link/0
        , init/1
        ]).

-define(CHILD(M), {M, {M, start_link, []}, permanent, 5000, worker, [M]}).


start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    CmSup = #{id => emqx_lwm2m_cm_sup,
              start => {emqx_lwm2m_cm_sup, start_link, []},
              restart => permanent,
              shutdown => infinity,
              type => supervisor,
              modules => [emqx_lwm2m_cm_sup]
            },
    {ok, { {one_for_all, 10, 3600}, [?CHILD(emqx_lwm2m_xml_object_db), CmSup] }}.
