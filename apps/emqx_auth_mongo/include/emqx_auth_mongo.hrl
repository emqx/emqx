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


-define(APP, emqx_auth_mongo).

-define(DEFAULT_SELECTORS, [{<<"username">>, <<"%u">>}]).

-record(superquery, {collection = <<"mqtt_user">>,
                     field      = <<"is_superuser">>,
                     selector   = {<<"username">>, <<"%u">>}}).

-record(authquery, {collection = <<"mqtt_user">>,
                    field      = <<"password">>,
                    hash       = sha256,
                    selector   = {<<"username">>, <<"%u">>}}).

-record(aclquery, {collection = <<"mqtt_acl">>,
                   selector   = {<<"username">>, <<"%u">>}}).
