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

-module(machinegun_hay_handler_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").

%% tests descriptions
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).

-export([no_workers_test/1]).
-export([exist_workers_test/1]).

-export([riak_pool_utilization_test/1]).

-define(NS, <<"NS">>).
-define(POOL_MAXCOUNT, 2).
-define(POOL_MAXQUEUE, 100).

%%
%% tests descriptions
%%
-type group_name() :: atom().
-type test_name() :: atom().
-type config() :: [{atom(), _}].

-spec all() -> [test_name() | {group, group_name()}].
all() ->
    [
        {group, with_gproc},
        {group, with_consuela},
        {group, riak}
    ].

-spec groups() -> [{group_name(), list(_), [test_name() | {group, group_name()}]}].
groups() ->
    [
        {with_gproc, [], [{group, base}]},
        {with_consuela, [], [{group, base}]},
        {riak, [], [
            riak_pool_utilization_test
        ]},
        {base, [], [
            no_workers_test,
            exist_workers_test
        ]}
    ].

%%
%% starting/stopping
%%
-spec init_per_suite(config()) -> config().
init_per_suite(C) ->
    C.

-spec end_per_suite(config()) -> ok.
end_per_suite(_C) ->
    ok.

-spec init_per_group(group_name(), config()) -> config().
init_per_group(with_gproc, C) ->
    Apps = machinegun_ct_helper:start_applications([gproc]),
    [{group_apps, Apps}, {registry, mg_core_procreg_gproc} | C];
init_per_group(with_consuela, C) ->
    Apps = machinegun_ct_helper:start_applications([consuela]),
    [{group_apps, Apps}, {registry, {mg_core_procreg_consuela, #{}}} | C];
init_per_group(GroupName, C) ->
    Storage =
        case GroupName of
            riak ->
                {mg_core_storage_riak, #{
                    host => "riakdb",
                    port => 8087,
                    bucket => ?NS,
                    pool_options => #{
                        init_count => 0,
                        max_count => ?POOL_MAXCOUNT,
                        queue_max => ?POOL_MAXQUEUE
                    },
                    sidecar =>
                        {machinegun_riak_hay, #{
                            interval => 500
                        }}
                }};
            base ->
                mg_core_storage_memory
        end,
    C1 = [
        {storage, Storage},
        {registry, mg_core_procreg_gproc}
        | C
    ],
    Config = machinegun_config(C1),
    Apps = machinegun_ct_helper:start_applications([
        {how_are_you, [
            {metrics_publishers, [machinegun_test_hay_publisher]},
            {metrics_handlers, [hay_vm_handler]}
        ]},
        {machinegun, Config}
    ]),
    {ok, ProcessorPid} = machinegun_test_processor:start(
        {0, 0, 0, 0},
        8023,
        genlib_map:compact(#{
            processor =>
                {"/processor", #{
                    signal => fun signal_handler/1,
                    call => fun call_handler/1
                }}
        })
    ),
    [
        {apps, Apps},
        {automaton_options, #{
            url => "http://localhost:8022",
            ns => ?NS,
            retry_strategy => undefined
        }},
        {processor_pid, ProcessorPid}
        | C1
    ].

-spec end_per_group(group_name(), config()) -> _.
end_per_group(base, C) ->
    ok = proc_lib:stop(?config(processor_pid, C)),
    machinegun_ct_helper:stop_applications(?config(apps, C));
end_per_group(riak, C) ->
    machinegun_ct_helper:stop_applications(?config(apps, C));
end_per_group(_, C) ->
    machinegun_ct_helper:stop_applications(?config(group_apps, C)).

-spec machinegun_config(config()) -> list().
machinegun_config(C) ->
    [
        {woody_server, #{ip => {0, 0, 0, 0}, port => 8022}},
        {namespaces, #{
            ?NS => #{
                storage => storage(C),
                processor => #{
                    url => <<"http://localhost:8023/processor">>,
                    transport_opts => #{pool => ns, max_connections => 100}
                },
                worker => #{
                    registry => registry(C),
                    sidecar => {machinegun_hay, #{interval => 100}}
                },
                default_processing_timeout => 5000,
                schedulers => #{
                    timers => #{}
                },
                retries => #{}
            }
        }},
        {event_sink_ns, #{
            storage => storage(C),
            registry => registry(C)
        }},
        {pulse,
            {machinegun_pulse, #{
                hay_options => #{enabled => true}
            }}}
    ].

-spec registry(config()) -> mg_core_procreg:options().
registry(C) ->
    ?config(registry, C).

-spec storage(config()) -> mg_core_storage:options().
storage(C) ->
    ?config(storage, C).

%% Tests

-spec no_workers_test(config()) -> _.
no_workers_test(_C) ->
    ok = timer:sleep(200),
    ?assertEqual(0, get_metric(gauge, [mg, workers, ?NS, number])).

-spec exist_workers_test(config()) -> _.
exist_workers_test(C) ->
    ok = machinegun_automaton_client:start(automaton_options(C), <<"exist_workers_test">>, []),
    ok = timer:sleep(200),
    ?assert(get_metric(gauge, [mg, workers, ?NS, number]) > 0).

-spec riak_pool_utilization_test(config()) -> _.
riak_pool_utilization_test(C) ->
    _ = [
        ok = machinegun_automaton_client:start(automaton_options(C), genlib:unique(), [])
     || _ <- lists:seq(1, ?POOL_MAXCOUNT * 10)
    ],
    ok = timer:sleep(2000),
    ?assertEqual(?POOL_MAXCOUNT, get_metric(counter, [mg, storage, ?NS, machines, pool, max_count])),
    ?assertEqual(?POOL_MAXQUEUE, get_metric(counter, [mg, storage, ?NS, machines, pool, queue_max])),
    ?assert(is_integer(get_metric(counter, [mg, storage, ?NS, machines, pool, in_use_count]))),
    ?assert(is_integer(get_metric(counter, [mg, storage, ?NS, machines, pool, free_count]))),
    ?assert(is_integer(get_metric(counter, [mg, storage, ?NS, machines, pool, queued_count]))),
    ?assert(is_integer(get_metric(counter, [mg, storage, ?NS, events, pool, in_use_count]))),
    ?assert(is_integer(get_metric(counter, [mg, storage, ?NS, events, pool, free_count]))),
    ?assert(is_integer(get_metric(counter, [mg, storage, ?NS, events, pool, queued_count]))).

%% Utils

-spec automaton_options(config()) -> _.
automaton_options(C) -> ?config(automaton_options, C).

%% Processor utils

-spec signal_handler(mg_core_events_machine:signal_args()) -> mg_core_events_machine:signal_result().
signal_handler({{init, _Args}, _Machine}) ->
    {{null(), []}, #{timer => timeout(0)}};
signal_handler({timeout, _Machine}) ->
    {{null(), []}, #{timer => timeout(0)}}.

-spec call_handler(mg_core_events_machine:call_args()) -> mg_core_events_machine:call_result().
call_handler({<<"foo">> = Args, _Machine}) ->
    {Args, {null(), [content(<<"bar">>)]}, #{}}.

-spec null() -> mg_core_events:content().
null() ->
    content(null).

-spec content(mg_core_storage:opaque()) -> mg_core_events:content().
content(Body) ->
    {#{format_version => 42}, Body}.

-spec timeout(non_neg_integer()) -> mg_core_events_machine:timer_action().
timeout(Timeout) ->
    {set_timer, {timeout, Timeout}, undefined, undefined}.

%% Metrics utils

-spec get_metric(how_are_you:metric_type(), how_are_you:metric_key()) ->
    how_are_you:metric_value() | undefined.
get_metric(Type, Key) ->
    machinegun_test_hay_publisher:lookup(Type, Key).
