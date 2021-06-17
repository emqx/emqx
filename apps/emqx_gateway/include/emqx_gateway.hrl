%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_GATEWAY_HRL).
-define(EMQX_GATEWAY_HRL, 1).


-type instance_id()   :: atom().
-type gateway_id()    :: atom().

%% @doc The Gateway Instace defination
-record(instance, { id     :: instance_id()
                  , gwid   :: gateway_id()
                  , order  :: non_neg_integer()  %% ??
                  , name   :: binary()
                  , descr  :: binary() | undefined
                  , rawconf = #{} :: map()
                  , enable = true :: boolean()  %% FIXME: Read from configuration ?
                  }).

-type instance() :: #instance{}.

%-type instance_info() ::
%        #{ id    := atom()
%         , type  := atom()
%         , order := non_neg_integer()
%         , name  := binary()
%         , descr => binary() | undefined
%         , status := stopped | running 
%         }.

%% @doc
% FIXME:
%-type clientinfo definations?
%-type conninfo definations?

-endif.
