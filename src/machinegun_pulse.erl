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

%% mg_pulse handler
-behaviour(mg_core_pulse).

-export([handle_beat/2]).

%% pulse types
-type beat() ::
    mg_core_pulse:beat()
    | mg_core_consuela_pulse_adapter:beat()
    | mg_core_queue_scanner:beat()
    | #woody_event{}
    | #woody_request_handle_error{}.

-type options() :: #{
    woody_event_handler_options => woody_event_handler:options(),
    lifecycle_kafka_options => machinegun_pulse_lifecycle_kafka:options()
}.

-export_type([beat/0]).
-export_type([options/0]).

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), beat()) -> ok.
handle_beat(Options, Beat) ->
    ok = machinegun_pulse_otel:handle_beat(Options, Beat),
    ok = machinegun_pulse_log:handle_beat(maps:get(woody_event_handler_options, Options, #{}), Beat),
    ok = machinegun_pulse_prometheus:handle_beat(#{}, Beat),
    ok = maybe_handle_lifecycle_kafka(Options, Beat).

%%
%% Internal
%%

-spec maybe_handle_lifecycle_kafka(options(), beat()) -> ok.
maybe_handle_lifecycle_kafka(#{lifecycle_kafka_options := KafkaOptions}, Beat) ->
    machinegun_pulse_lifecycle_kafka:handle_beat(KafkaOptions, Beat);
maybe_handle_lifecycle_kafka(_Options, _Beat) ->
    %% kafka lifecycle pulse is disabled
    ok.
