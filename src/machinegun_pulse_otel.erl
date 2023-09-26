-module(machinegun_pulse_otel).

-include_lib("machinegun_core/include/pulse.hrl").
-include_lib("machinegun_woody_api/include/pulse.hrl").
-include_lib("woody/src/woody_defs.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

%% mg_pulse handler
-behaviour(mg_core_pulse).

-export([handle_beat/2]).

%% TODO Specify available options if any
-type options() :: map().

-export_type([options/0]).

%%
%% mg_pulse handler
%%

-spec handle_beat(options(), machinegun_pulse:beat()) -> ok.
%%
%% Consuela (registry, discovery etc) beats
%% ============================================================================
%%
handle_beat(_Options, _Beat = {consuela, {client, {request, _Request}}}) ->
    ok;
handle_beat(_Options, _Beat = {consuela, {client, {result, _Result}}}) ->
    ok;
handle_beat(_Options, _Beat = {consuela, {registry_server, _RegistryBeat}}) ->
    ok;
handle_beat(_Options, _Beat = {consuela, {session_keeper, _SessionKeeperBeat}}) ->
    ok;
handle_beat(_Options, _Beat = {consuela, {zombie_reaper, _ZombieReaperBeat}}) ->
    ok;
handle_beat(_Options, _Beat = {consuela, {discovery_server, _DiscoveryBeat}}) ->
    ok;
handle_beat(_Options, _Beat = {consuela, {presence_session, _PresenceSessionBeat}}) ->
    ok;
%%
%% Queue scanner beats
%% ============================================================================
%%
handle_beat(_Options, _Beat = {squad, {_Squad, {rank, {changed, _Rank}}, _ExtraMeta}}) ->
    ok;
handle_beat(_Options, _Beat = {squad, {_Squad, {{member, _Pid}, _Change}, _ExtraMeta}}) ->
    ok;
handle_beat(_Options, _Beat = {squad, {_Squad, {{broadcast, _Payload}, _SentOrReceived}, _ExtraMeta}}) ->
    ok;
handle_beat(_Options, _Beat = {squad, {_Squad, {{timer, _Ref}, _TimerAction}, _ExtraMeta}}) ->
    ok;
handle_beat(_Options, _Beat = {squad, {_Squad, {{monitor, _Ref}, _MonitorAction}, _ExtraMeta}}) ->
    ok;
%%
%% Woody API beats
%% ============================================================================
%%
handle_beat(
    _Options,
    #woody_event{
        event = ?EV_CALL_SERVICE,
        rpc_id = #{span_id := WoodySpanId},
        event_meta = #{service := Service, function := Function}
    }
) ->
    SpanName = <<"woody_call ", (atom_to_binary(Service))/binary, ":", (atom_to_binary(Function))/binary>>,
    proc_span_start(WoodySpanId, SpanName, #{kind => ?SPAN_KIND_CLIENT, attributes => #{}});
handle_beat(_Options, #woody_event{event = ?EV_SERVICE_RESULT, rpc_id = #{span_id := WoodySpanId}}) ->
    proc_span_end(WoodySpanId);
handle_beat(
    _Options,
    #woody_event{
        event = ?EV_INVOKE_SERVICE_HANDLER,
        rpc_id = #{span_id := WoodySpanId},
        event_meta = #{service := Service, function := Function}
    }
) ->
    SpanName = <<"woody_invoke ", (atom_to_binary(Service))/binary, ":", (atom_to_binary(Function))/binary>>,
    proc_span_start(WoodySpanId, SpanName, #{kind => ?SPAN_KIND_SERVER, attributes => #{}});
handle_beat(_Options, #woody_event{event = ?EV_SERVICE_HANDLER_RESULT, rpc_id = #{span_id := WoodySpanId}}) ->
    proc_span_end(WoodySpanId);
handle_beat(_Options, _Beat = #woody_event{}) ->
    ok;
%% Woody server's function handling error beat.
handle_beat(_Options, _Beat = #woody_request_handle_error{}) ->
    %% TODO If in span add attributes etc
    ok;
%%
%% Machinegun core beats
%% ============================================================================
%%
%% Timer
%% Event machine action 'set_timer' performed.
handle_beat(_Options, Beat = #mg_core_timer_lifecycle_created{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% In case of transient error during processing 'timeout' machine will try to
%% reschedule its next action according to configured 'timers' retry strategy.
%% Then it transitions to new state with status 'retrying' and emits this beat.
handle_beat(_Options, _Beat = #mg_core_timer_lifecycle_rescheduled{}) ->
    ok;
%% Since rescheduling produces new state transition, it can fail transiently.
%% If thrown exception has types 'transient' or 'timeout' then this beat is
%% emitted.
handle_beat(_Options, _Beat = #mg_core_timer_lifecycle_rescheduling_error{}) ->
    ok;
%% Event machine timer removed: action 'unset_timer' performed.
handle_beat(_Options, Beat = #mg_core_timer_lifecycle_removed{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Scheduler handling
%% ???
handle_beat(_Options, _Beat = #mg_core_scheduler_task_add_error{}) ->
    ok;
%% Tasks queue scan success.
handle_beat(_Options, _Beat = #mg_core_scheduler_search_success{}) ->
    ok;
%% Tasks queue scan failure.
handle_beat(_Options, _Beat = #mg_core_scheduler_search_error{}) ->
    ok;
%% Task execution failed.
handle_beat(_Options, _Beat = #mg_core_scheduler_task_error{}) ->
    ok;
%% Emits this beat when one or many new tasks were successfully enqueued.
handle_beat(_Options, _Beat = #mg_core_scheduler_new_tasks{}) ->
    ok;
%% Starting task execution.
handle_beat(_Options, _Beat = #mg_core_scheduler_task_started{}) ->
    ok;
%% Task execution finished.
handle_beat(_Options, _Beat = #mg_core_scheduler_task_finished{}) ->
    ok;
%% TODO resource and quota management
handle_beat(_Options, _Beat = #mg_core_scheduler_quota_reserved{}) ->
    ok;
%% Timer handling
%% Wraps `Module:process_machine/7` when processor impact is 'timeout'.
handle_beat(_Options, #mg_core_timer_process_started{machine_id = ID, namespace = NS, queue = Queue}) ->
    proc_span_start({<<"mg_core_timer_process">>, Queue}, <<"mg_core_timer_process">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{<<"machine.ns">> => NS, <<"machine.id">> => ID, <<"queue">> => atom_to_binary(Queue)}
    });
handle_beat(_Options, _Beat = #mg_core_timer_process_finished{queue = Queue}) ->
    proc_span_end({<<"mg_core_timer_process">>, Queue});
%% Machine process state
%% Machine created and loaded
%% Mind that loading of machine state happens in its worker' process context
%% and not during call to supervisor.
handle_beat(_Options, Beat = #mg_core_machine_lifecycle_created{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Removal of machine (from storage); signalled by 'remove' action in a new
%% processed state.
handle_beat(_Options, Beat = #mg_core_machine_lifecycle_removed{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Existing machine loaded.
handle_beat(_Options, Beat = #mg_core_machine_lifecycle_loaded{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% When machine's worker process handles scheduled timeout timer and stops
%% normally.
handle_beat(_Options, Beat = #mg_core_machine_lifecycle_unloaded{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Machine can be configured with probability of suicide via
%% "erlang:exit(self(), kill)". Each time machine successfully completes
%% `Module:process_machine/7` call and before persisting transition artifacts
%% (including its very own new state snapshot), it attempts a suicide.
handle_beat(_Options, Beat = #mg_core_machine_lifecycle_committed_suicide{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% When existing machine with nonerroneous state fails to handle processor
%% response it transitions to special 'failed' state.
%% See `mg_core_machine:machine_status/0`:
%% "{error, Reason :: term(), machine_regular_status()}".
%% NOTE Nonexisting machine can also fail on init.
handle_beat(_Options, #mg_core_machine_lifecycle_failed{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    maybe_record_exception(otel_tracer:current_span_ctx(), Exception, #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% This event occrurs once existing machine successfully transitions from
%% special 'failed' state, but before it's new state persistence in storage.
handle_beat(_Options, Beat = #mg_core_machine_lifecycle_repaired{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% When failed to load machine.
handle_beat(_Options, #mg_core_machine_lifecycle_loading_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    maybe_record_exception(otel_tracer:current_span_ctx(), Exception, #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Transient error when removing or persisting machine state transition.
handle_beat(_Options, #mg_core_machine_lifecycle_transient_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    maybe_record_exception(otel_tracer:current_span_ctx(), Exception, #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Machine call handling
%% Wraps core machine call `Module:process_machine/7`.
handle_beat(
    _Options,
    #mg_core_machine_process_started{processor_impact = ProcessorImpact, machine_id = ID, namespace = NS}
) ->
    proc_span_start(ProcessorImpact, <<"mg_core_machine_process">>, #{
        kind => ?SPAN_KIND_INTERNAL,
        attributes => #{<<"machine.ns">> => NS, <<"machine.id">> => ID}
    });
handle_beat(_Options, #mg_core_machine_process_finished{processor_impact = ProcessorImpact}) ->
    proc_span_end(ProcessorImpact);
%% Transient error _throw_n from during state processing.
handle_beat(_Options, #mg_core_machine_process_transient_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    maybe_record_exception(otel_tracer:current_span_ctx(), Exception, #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Machine notification
handle_beat(_Options, Beat = #mg_core_machine_notification_created{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
handle_beat(_Options, Beat = #mg_core_machine_notification_delivered{machine_id = ID, namespace = NS}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
handle_beat(_Options, #mg_core_machine_notification_delivery_error{
    exception = Exception, machine_id = ID, namespace = NS
}) ->
    maybe_record_exception(otel_tracer:current_span_ctx(), Exception, #{
        <<"machine.ns">> => NS, <<"machine.id">> => ID
    });
%% Machine worker handling
%% Happens upon worker's gen_server call.
handle_beat(_Options, Beat = #mg_core_worker_call_attempt{}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{});
%% Upon worker's gen_server start.
handle_beat(_Options, Beat = #mg_core_worker_start_attempt{}) ->
    maybe_add_span_event(otel_tracer:current_span_ctx(), atom_to_binary(element(1, Beat)), #{});
%% Storage calls
handle_beat(_Options, #mg_core_storage_get_start{name = Name}) ->
    proc_span_start({<<"mg_core_storage_get">>, Name}, <<"mg_core_storage_get">>, #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_get_finish{name = Name}) ->
    proc_span_end({<<"mg_core_storage_get">>, Name});
handle_beat(_Options, #mg_core_storage_put_start{name = Name}) ->
    proc_span_start({<<"mg_core_storage_put">>, Name}, <<"mg_core_storage_put">>, #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_put_finish{name = Name}) ->
    proc_span_end({<<"mg_core_storage_put">>, Name});
handle_beat(_Options, #mg_core_storage_search_start{name = Name}) ->
    proc_span_start({<<"mg_core_storage_search">>, Name}, <<"mg_core_storage_search">>, #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_search_finish{name = Name}) ->
    proc_span_end({<<"mg_core_storage_search">>, Name});
handle_beat(_Options, #mg_core_storage_delete_start{name = Name}) ->
    proc_span_start({<<"mg_core_storage_delete">>, Name}, <<"mg_core_storage_delete">>, #{kind => ?SPAN_KIND_INTERNAL});
handle_beat(_Options, #mg_core_storage_delete_finish{name = Name}) ->
    proc_span_end({<<"mg_core_storage_delete">>, Name});
%% Event sink operations
%% Kafka specific producer event.
handle_beat(_Options, _Beat = #mg_core_events_sink_kafka_sent{}) ->
    ok;
%% Riak client call handling
%% Riak specific beats, correlate with storage calls.
handle_beat(_Options, _Beat = #mg_core_riak_client_get_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_client_get_finish{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_client_put_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_client_put_finish{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_client_search_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_client_search_finish{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_client_delete_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_client_delete_finish{}) ->
    ok;
%% Riak client call handling
%% Riak specific connection pool events.
handle_beat(_Options, _Beat = #mg_core_riak_connection_pool_state_reached{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_connection_pool_connection_killed{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_riak_connection_pool_error{}) ->
    ok;
%% Disregard any other
handle_beat(_Options, _Beat) ->
    ok.

%% Internal

-spec proc_span_start(term(), opentelemetry:span_name(), otel_span:start_opts()) ->
    ok.
proc_span_start(SpanKey, SpanName, Opts) ->
    Tracer = opentelemetry:get_application_tracer(?MODULE),
    SpanCtx = otel_tracer:set_current_span(otel_tracer:start_span(Tracer, SpanName, Opts)),
    _ = erlang:put({proc_span_ctx, SpanKey}, SpanCtx),
    ok.

-spec proc_span_end(term()) -> ok.
proc_span_end(SpanKey) ->
    case erlang:erase({proc_span_ctx, SpanKey}) of
        undefined ->
            ok;
        #span_ctx{} = SpanCtx ->
            _ = otel_tracer:set_current_span(otel_span:end_span(SpanCtx, undefined)),
            ok
    end.

-spec maybe_add_span_event(opentelemetry:span_ctx(), opentelemetry:event_name(), opentelemetry:attributes_map()) -> ok.
maybe_add_span_event(undefined, _EventName, _EventAttributes) ->
    ok;
maybe_add_span_event(SpanCtx, EventName, EventAttributes) ->
    otel_span:add_event(SpanCtx, EventName, EventAttributes),
    ok.

-spec maybe_record_exception(opentelemetry:span_ctx(), mg_core_utils:exception(), opentelemetry:attributes_map()) -> ok.
maybe_record_exception(undefined, _Exception, _Attributes) ->
    ok;
maybe_record_exception(SpanCtx, {Class, Reason, Stacktrace}, Attributes) ->
    otel_span:record_exception(SpanCtx, Class, Reason, Stacktrace, Attributes),
    ok.
