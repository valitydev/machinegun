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

-type woody_event() :: #woody_event{}.

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
handle_beat(_Options, Beat = #woody_event{event = ?EV_CALL_SERVICE}) ->
    woody_span_start(Beat);
handle_beat(_Options, Beat = #woody_event{event = ?EV_SERVICE_RESULT}) ->
    woody_span_end(Beat);
handle_beat(_Options, Beat = #woody_event{event = ?EV_INVOKE_SERVICE_HANDLER}) ->
    woody_span_start(Beat);
handle_beat(_Options, Beat = #woody_event{event = ?EV_SERVICE_HANDLER_RESULT}) ->
    woody_span_end(Beat);
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
handle_beat(_Options, _Beat = #mg_core_timer_lifecycle_created{}) ->
    ok;
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
handle_beat(_Options, _Beat = #mg_core_timer_lifecycle_removed{}) ->
    ok;
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
handle_beat(_Options, _Beat = #mg_core_timer_process_started{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_timer_process_finished{}) ->
    ok;
%% Machine process state
%% Machine created and loaded
%% Mind that loading of machine state happens in its worker' process context
%% and not during call to supervisor.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_created{}) ->
    ok;
%% Removal of machine (from storage); signalled by 'remove' action in a new
%% processed state.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_removed{}) ->
    ok;
%% Existing machine loaded.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_loaded{}) ->
    ok;
%% When machine's worker process handles scheduled timeout timer and stops
%% normally.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_unloaded{}) ->
    ok;
%% Machine can be configured with probability of suicide via
%% "erlang:exit(self(), kill)". Each time machine successfully completes
%% `Module:process_machine/7` call and before persisting transition artifacts
%% (including its very own new state snapshot), it attempts a suicide.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_committed_suicide{}) ->
    ok;
%% When existing machine with nonerroneous state fails to handle processor
%% response it transitions to special 'failed' state.
%% See `mg_core_machine:machine_status/0`:
%% "{error, Reason :: term(), machine_regular_status()}".
%% NOTE Nonexisting machine can also fail on init.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_failed{}) ->
    ok;
%% This event occrurs once existing machine successfully transitions from
%% special 'failed' state, but before it's new state persistence in storage.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_repaired{}) ->
    ok;
%% When failed to load machine.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_loading_error{}) ->
    ok;
%% Transient error when removing or perisisting machine state transition.
handle_beat(_Options, _Beat = #mg_core_machine_lifecycle_transient_error{}) ->
    ok;
%% Machine call handling
%% Wraps core machine call `Module:process_machine/7`.
handle_beat(_Options, _Beat = #mg_core_machine_process_started{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_machine_process_finished{}) ->
    ok;
%% Transient error _throw_n from during state processing.
handle_beat(_Options, _Beat = #mg_core_machine_process_transient_error{}) ->
    ok;
%% Machine notification
handle_beat(_Options, _Beat = #mg_core_machine_notification_created{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_machine_notification_delivered{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_machine_notification_delivery_error{}) ->
    ok;
%% Machine worker handling
%% Happens upon worker's gen_server call.
handle_beat(_Options, _Beat = #mg_core_worker_call_attempt{}) ->
    ok;
%% Upon worker's gen_server start.
handle_beat(_Options, _Beat = #mg_core_worker_start_attempt{}) ->
    ok;
%% Storage calls
handle_beat(_Options, _Beat = #mg_core_storage_get_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_storage_get_finish{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_storage_put_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_storage_put_finish{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_storage_search_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_storage_search_finish{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_storage_delete_start{}) ->
    ok;
handle_beat(_Options, _Beat = #mg_core_storage_delete_finish{}) ->
    ok;
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

-spec woody_span_start(woody_event()) -> ok.
woody_span_start(Beat) ->
    _ = proc_span_start(woody_span_key(Beat), woody_span_name(Beat), woody_span_opts(Beat)),
    ok.

-spec woody_span_end(woody_event()) -> ok.
woody_span_end(Beat) ->
    _ = proc_span_end(woody_span_key(Beat)),
    ok.

-spec woody_span_name(woody_event()) -> opentelemetry:span_name().
% Rely on service/function metadata
woody_span_name(#woody_event{
    event = ?EV_CALL_SERVICE,
    event_meta = #{service := Service, function := Function}
}) ->
    <<"woody call ", (atom_to_binary(Service))/binary, ":", (atom_to_binary(Function))/binary>>;
woody_span_name(#woody_event{
    event = ?EV_INVOKE_SERVICE_HANDLER,
    event_meta = #{service := Service, function := Function}
}) ->
    <<"woody invoke ", (atom_to_binary(Service))/binary, ":", (atom_to_binary(Function))/binary>>;
woody_span_name(#woody_event{event = EventType}) ->
    <<"unknown woody span '", (atom_to_binary(EventType))/binary, "'">>.

-spec woody_span_opts(woody_event()) -> otel_span:start_opts().
%% NOTE Mind attributes spec in otel_span:start_opts/0
%%          #{...,
%%            attributes => #{binary() => binary()}}
%%      Else tracer will silently ignore invalid keys/values
woody_span_opts(#woody_event{event = ?EV_CALL_SERVICE}) ->
    #{
        kind => ?SPAN_KIND_CLIENT
    };
woody_span_opts(#woody_event{event = ?EV_INVOKE_SERVICE_HANDLER}) ->
    #{
        kind => ?SPAN_KIND_SERVER
    }.

-spec woody_span_key(woody_event()) -> woody:req_id() | undefined.
woody_span_key(#woody_event{rpc_id = #{span_id := WoodySpanReqId}}) ->
    WoodySpanReqId;
woody_span_key(_Beat) ->
    undefined.

-spec proc_span_start(woody:req_id() | undefined, opentelemetry:span_name(), otel_span:start_opts()) ->
    opentelemetry:span_ctx() | undefined.
proc_span_start(undefined, _SpanName, _Opts) ->
    undefined;
proc_span_start(SpanKey, SpanName, Opts) ->
    Tracer = opentelemetry:get_application_tracer(?MODULE),
    SpanCtx = otel_tracer:set_current_span(otel_tracer:start_span(Tracer, SpanName, Opts)),
    _ = erlang:put({proc_span_ctx, SpanKey}, SpanCtx),
    SpanCtx.

-spec proc_span_end(woody:req_id() | undefined) -> opentelemetry:span_ctx() | undefined.
proc_span_end(undefined) ->
    undefined;
proc_span_end(SpanKey) ->
    case erlang:erase({proc_span_ctx, SpanKey}) of
        undefined -> undefined;
        SpanCtx -> otel_tracer:set_current_span(otel_span:end_span(SpanCtx, undefined))
    end.
