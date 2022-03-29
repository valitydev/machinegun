%%%
%%% Copyright 2020 RBKmoney
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%%

-module(machinegun_pulse).

-include_lib("machinegun_woody_api/include/pulse.hrl").
-include_lib("machinegun_core/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_core_pulse).

-export([handle_beat/2]).

%% pulse types
-type beat() ::
    mg_core_pulse:beat()
    | mg_core_consuela_pulse_adapter:beat()
    | mg_core_queue_scanner:beat()
    | #woody_event{}
    | #woody_request_handle_error{}
    %% TODO TD-229: Move it to `mg_core_pulse:beat()`
    | #mg_core_scheduler_search_success{}
    | #mg_core_riak_client_get_start{}
    | #mg_core_riak_client_get_finish{}
    | #mg_core_riak_client_put_start{}
    | #mg_core_riak_client_put_finish{}
    | #mg_core_riak_client_search_start{}
    | #mg_core_riak_client_search_finish{}
    | #mg_core_riak_client_delete_start{}
    | #mg_core_riak_client_delete_finish{}.

-type options() :: #{
    hay_options := machinegun_pulse_hay:options(),
    woody_event_handler_options := woody_event_handler:options()
}.

-export_type([beat/0]).
-export_type([options/0]).

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) -> ok.
handle_beat(Options, Beat) ->
    ok = machinegun_pulse_log:handle_beat(maps:get(woody_event_handler_options, Options), Beat),
    ok = machinegun_pulse_hay:handle_beat(maps:get(hay_options, Options), Beat),
    ok = machinegun_pulse_prometheus:handle_beat(Options, Beat).
