%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules).

-export([
    get_advanced_mqtt_features_in_use/0,
    set_advanced_mqtt_features_in_use/1
]).

-type advanced_mqtt_feature() :: delayed | topic_rewrite | retained | auto_subscribe.
-type advanced_mqtt_features_in_use() :: #{advanced_mqtt_feature() => boolean()}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec get_advanced_mqtt_features_in_use() -> advanced_mqtt_features_in_use().
get_advanced_mqtt_features_in_use() ->
    application:get_env(?MODULE, advanced_mqtt_features_in_use, #{}).

-spec set_advanced_mqtt_features_in_use(advanced_mqtt_features_in_use()) -> ok.
set_advanced_mqtt_features_in_use(Features) ->
    application:set_env(?MODULE, advanced_mqtt_features_in_use, Features).
