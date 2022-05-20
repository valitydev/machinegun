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

-module(machinegun_hay_metric_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("machinegun_core/include/pulse.hrl").

%% tests descriptions
-export([all/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).

-export([offset_bin_metric_test/1]).
-export([fraction_and_queue_bin_metric_test/1]).
-export([duration_bin_metric_test/1]).

-define(NS, <<"NS">>).

%%
%% tests descriptions
%%
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name()].
all() ->
    [
        offset_bin_metric_test,
        fraction_and_queue_bin_metric_test,
        duration_bin_metric_test
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    Apps = machinegun_ct_helper:start_applications([
        gproc,
        {how_are_you, [
            {metrics_publishers, []},
            {metrics_handlers, []}
        ]}
    ]),
    [{apps, Apps} | C].

-spec end_per_suite(config()) -> ok.
end_per_suite(C) ->
    machinegun_ct_helper:stop_applications(?config(apps, C)).

%% Tests

-spec offset_bin_metric_test(config()) -> _.
offset_bin_metric_test(_C) ->
    Offsets = [erlang:trunc(-10 + math:pow(2, I)) || I <- lists:seq(0, 10, 1)],
    _ = [
        ok = test_beat(#mg_core_timer_lifecycle_created{
            namespace = ?NS,
            target_timestamp = genlib_time:unow() + Offset,
            machine_id = <<"ID">>,
            request_context = null
        })
     || Offset <- Offsets
    ].

-spec fraction_and_queue_bin_metric_test(config()) -> _.
fraction_and_queue_bin_metric_test(_C) ->
    Samples = lists:seq(0, 200, 1),
    _ = [
        ok = test_beat(#mg_core_worker_start_attempt{
            namespace = ?NS,
            msg_queue_len = Sample,
            msg_queue_limit = 100,
            machine_id = <<"ID">>,
            request_context = null
        })
     || Sample <- Samples
    ].

-spec duration_bin_metric_test(config()) -> _.
duration_bin_metric_test(_C) ->
    Samples = [erlang:trunc(math:pow(2, I)) || I <- lists:seq(0, 20, 1)],
    _ = [
        ok = test_beat(#mg_core_machine_process_finished{
            namespace = ?NS,
            duration = Sample,
            processor_impact = {init, []},
            machine_id = <<"ID">>,
            request_context = null
        })
     || Sample <- Samples
    ].

%% Metrics utils

-spec test_beat(term()) -> ok.
test_beat(Beat) ->
    machinegun_pulse_hay:handle_beat(#{enabled => true}, Beat).
