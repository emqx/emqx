%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_json).

-export([encode/1, encode/2, decode/1, decode/2]).

-spec(encode(jsx:json_term()) -> jsx:json_text()).
encode(Term) ->
    jsx:encode(Term).

-spec(encode(jsx:json_term(), jsx_to_json:config()) -> jsx:json_text()).
encode(Term, Opts) ->
    jsx:encode(Term, Opts).

-spec(decode(jsx:json_text()) -> jsx:json_term()).
decode(JSON) ->
    jsx:decode(JSON).

-spec(decode(jsx:json_text(), jsx_to_json:config()) -> jsx:json_term()).
decode(JSON, Opts) ->
    jsx:decode(JSON, Opts).

