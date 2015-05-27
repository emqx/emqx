%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
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
%%% eMQTT System Topics.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-define(SYSTOP, <<"$SYS">>).

%%------------------------------------------------------------------------------
%% $SYS Topics of Broker
%%------------------------------------------------------------------------------
-define(SYSTOP_BROKERS, [
    version,      % Broker version
    uptime,       % Broker uptime
    datetime,     % Broker local datetime
    sysdescr      % Broker description
]).

%%------------------------------------------------------------------------------
%% $SYS Topics for Clients
%%------------------------------------------------------------------------------
-define(SYSTOP_CLIENTS, [
    'clients/count',         % clients connected current
    'clients/max'            % max clients connected
]).

%%------------------------------------------------------------------------------
%% $SYS Topics for Sessions
%%------------------------------------------------------------------------------
-define(SYSTOP_SESSIONS, [
    'sessions/count',
    'sessions/max'
]).

%%------------------------------------------------------------------------------
%% $SYS Topics for Subscribers
%%------------------------------------------------------------------------------
-define(SYSTOP_PUBSUB, [
    'topics/count',      % ...
    'topics/max',        % ...
    'subscribers/count', % ...
    'subscribers/max',   % ...
    'queues/count',      % ...
    'queues/max'         % ...
]).

%%------------------------------------------------------------------------------
%% Bytes sent and received of Broker
%%------------------------------------------------------------------------------
-define(SYSTOP_BYTES, [
    {counter, 'bytes/received'},   % Total bytes received
    {counter, 'bytes/sent'}        % Total bytes sent
]).

%%------------------------------------------------------------------------------
%% Packets sent and received of Broker
%%------------------------------------------------------------------------------
-define(SYSTOP_PACKETS, [
    {counter, 'packets/received'},         % All Packets received
    {counter, 'packets/sent'},             % All Packets sent
    {counter, 'packets/connect'},          % CONNECT Packets received
    {counter, 'packets/connack'},          % CONNACK Packets sent
    {counter, 'packets/publish/received'}, % PUBLISH packets received
    {counter, 'packets/publish/sent'},     % PUBLISH packets sent
    {counter, 'packets/subscribe'},        % SUBSCRIBE Packets received 
    {counter, 'packets/suback'},           % SUBACK packets sent 
    {counter, 'packets/unsubscribe'},      % UNSUBSCRIBE Packets received
    {counter, 'packets/unsuback'},         % UNSUBACK Packets sent
    {counter, 'packets/pingreq'},          % PINGREQ packets received
    {counter, 'packets/pingresp'},         % PINGRESP Packets sent
    {counter, 'packets/disconnect'}        % DISCONNECT Packets received 
]).

%%------------------------------------------------------------------------------
%% Messages sent and received of broker
%%------------------------------------------------------------------------------
-define(SYSTOP_MESSAGES, [
    {counter, 'messages/received'},      % Messages received
    {counter, 'messages/sent'},          % Messages sent
    {gauge,   'messages/retained/count'},% Messagea retained
    {gauge,   'messages/stored/count'},  % Messages stored
    {counter, 'messages/dropped'}        % Messages dropped
]).


