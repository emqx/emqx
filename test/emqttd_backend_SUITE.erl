%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

-module(emqttd_backend_SUITE).

-compile(export_all).

all() -> [{group, retainer}].

groups() -> [{retainer, [], [t_retain]}].

init_per_group(retainer, _Config) ->
    ok = emqttd_mnesia:ensure_started(),
    emqttd_retainer:mnesia(boot),
    emqttd_retainer:mnesia(copy).

end_per_group(retainer, _Config) ->
    ok;
end_per_group(_Group, _Config) ->
    ok.

t_retain(_) -> ok.

