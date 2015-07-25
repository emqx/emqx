%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% emqttd Qos Functions.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_qos).

-include("emqttd_protocol.hrl").

-export([a/1, i/1]).

a(?QOS_0) -> qos0;
a(?QOS_1) -> qos1;
a(?QOS_2) -> qos2;
a(qos0)   -> qos0;
a(qos1)   -> qos1;
a(qos2)   -> qos2.

i(?QOS_0) -> ?QOS_0;
i(?QOS_1) -> ?QOS_1;
i(?QOS_2) -> ?QOS_2;
i(qos0)   -> ?QOS_0;
i(qos1)   -> ?QOS_1;
i(qos2)   -> ?QOS_2.


