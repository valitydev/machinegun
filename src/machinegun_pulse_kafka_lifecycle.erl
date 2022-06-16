%%%
%%% Copyright 2022 Valitydev
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

-module(machinegun_pulse_kafka_lifecycle).

-include_lib("machinegun_core/include/pulse.hrl").
-include_lib("machinegun_woody_api/include/pulse.hrl").

%% mg_pulse handler
-behaviour(mg_core_pulse).

-type options() :: #{
    topic := brod:topic(),
    client := brod:client(),
    encoder := encoder()
}.

-export([handle_beat/2]).
-export_type([options/0]).

%% internal types
-type beat() :: machinegun_pulse:beat().
-type encoder() :: fun((mg_core:ns(), mg_core:id(), beat_event()) -> iodata()).

%% FIXME: This should reside at machinegun_core level, but since
%% lifecycle kafka over pulse is a *temporary* solution it is probably fine here.
-type beat_event() :: any().

%%%
%% mg_pulse handler
%%

-spec handle_beat(options() | undefined, beat()) -> ok.
handle_beat(undefined, _) ->
    ok;
handle_beat(Options, Beat) when
    is_record(Beat, mg_core_machine_lifecycle_created) orelse
        is_record(Beat, mg_core_machine_lifecycle_failed) orelse
        is_record(Beat, mg_core_machine_lifecycle_removed)
->
    #{client := Client, topic := Topic, encoder := Encoder} = Options,
    {SourceNS, SourceID, Event} = get_beat_data(Beat),
    Batch = encode(Encoder, SourceNS, SourceID, Event),
    {ok, _Partition, _Offset} = produce(Client, Topic, event_key(SourceNS, SourceID), Batch),
    ok;
handle_beat(_, _) ->
    ok.

%% Internals

-spec get_beat_data(beat()) -> {mg_core:ns(), mg_core:id(), beat_event()}.
get_beat_data(#mg_core_machine_lifecycle_created{namespace = NS, machine_id = ID}) ->
    {NS, ID, {machine_lifecycle_created, #{occurred_at => ts_now()}}};
get_beat_data(#mg_core_machine_lifecycle_failed{namespace = NS, machine_id = ID, exception = Exception}) ->
    {NS, ID, {machine_lifecycle_failed, #{occurred_at => ts_now(), exception => Exception}}};
get_beat_data(#mg_core_machine_lifecycle_removed{namespace = NS, machine_id = ID}) ->
    {NS, ID, {machine_lifecycle_removed, #{occurred_at => ts_now()}}}.

-spec ts_now() -> integer().
ts_now() ->
    os:system_time(nanosecond).

-spec event_key(mg_core:ns(), mg_core:id()) -> term().
event_key(NS, MachineID) ->
    <<NS/binary, " ", MachineID/binary>>.

-spec encode(encoder(), mg_core:ns(), mg_core:id(), beat_event()) -> brod:batch_input().
encode(Encoder, SourceNS, SourceID, Event) ->
    [
        #{
            key => event_key(SourceNS, SourceID),
            value => Encoder(SourceNS, SourceID, Event)
        }
    ].

-spec produce(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    {ok, brod:partition(), brod:offset()}.
produce(Client, Topic, Key, Batch) ->
    case do_produce(Client, Topic, Key, Batch) of
        {ok, _Partition, _Offset} = Result ->
            Result;
        {error, Reason} ->
            handle_produce_error(Reason)
    end.

-spec do_produce(brod:client(), brod:topic(), brod:key(), brod:batch_input()) ->
    {ok, brod:partition(), brod:offset()} | {error, Reason :: any()}.
do_produce(Client, Topic, PartitionKey, Batch) ->
    try brod:get_partitions_count(Client, Topic) of
        {ok, PartitionsCount} ->
            Partition = partition(PartitionsCount, PartitionKey),
            case brod:produce_sync_offset(Client, Topic, Partition, PartitionKey, Batch) of
                {ok, Offset} ->
                    {ok, Partition, Offset};
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    catch
        exit:Reason ->
            {error, {exit, Reason}}
    end.

-spec handle_produce_error(atom()) -> no_return().
handle_produce_error(timeout) ->
    erlang:throw({transient, timeout});
handle_produce_error({exit, {Reasons = [_ | _], _}}) ->
    case lists:any(fun is_connectivity_reason/1, Reasons) of
        true ->
            erlang:throw({transient, {event_sink_unavailable, {connect_failed, Reasons}}});
        false ->
            erlang:error({?MODULE, {unexpected, Reasons}})
    end;
handle_produce_error({producer_down, Reason}) ->
    erlang:throw({transient, {event_sink_unavailable, {producer_down, Reason}}});
handle_produce_error(Reason) ->
    KnownErrors = #{
        % See https://kafka.apache.org/protocol.html#protocol_error_codes for kafka error details
        client_down => transient,
        unknown_server_error => unknown,
        corrupt_message => transient,
        unknown_topic_or_partition => transient,
        leader_not_available => transient,
        not_leader_for_partition => transient,
        request_timed_out => transient,
        broker_not_available => transient,
        replica_not_available => transient,
        message_too_large => misconfiguration,
        stale_controller_epoch => transient,
        network_exception => transient,
        invalid_topic_exception => logic,
        record_list_too_large => misconfiguration,
        not_enough_replicas => transient,
        not_enough_replicas_after_append => transient,
        invalid_required_acks => transient,
        topic_authorization_failed => misconfiguration,
        cluster_authorization_failed => misconfiguration,
        invalid_timestamp => logic,
        unsupported_sasl_mechanism => misconfiguration,
        illegal_sasl_state => logic,
        unsupported_version => misconfiguration,
        reassignment_in_progress => transient
    },
    case maps:find(Reason, KnownErrors) of
        {ok, transient} ->
            erlang:throw({transient, {event_sink_unavailable, Reason}});
        {ok, misconfiguration} ->
            erlang:throw({transient, {event_sink_misconfiguration, Reason}});
        {ok, Other} ->
            erlang:error({?MODULE, {Other, Reason}});
        error ->
            erlang:error({?MODULE, {unexpected, Reason}})
    end.

-spec is_connectivity_reason(
    {inet:hostname(), {inet:posix() | {failed_to_upgrade_to_ssl, _SSLError}, _ST}}
) ->
    boolean().
is_connectivity_reason({_, {timeout, _ST}}) ->
    true;
is_connectivity_reason({_, {econnrefused, _ST}}) ->
    true;
is_connectivity_reason({_, {ehostunreach, _ST}}) ->
    true;
is_connectivity_reason({_, {enetunreach, _ST}}) ->
    true;
is_connectivity_reason({_, {nxdomain, _ST}}) ->
    true;
is_connectivity_reason({_, {{failed_to_upgrade_to_ssl, _SSLError}, _ST}}) ->
    true;
is_connectivity_reason({_, {{_, closed}, _ST}}) ->
    true;
is_connectivity_reason({_, {{_, timeout}, _ST}}) ->
    true;
is_connectivity_reason(_Reason) ->
    false.

-spec partition(non_neg_integer(), brod:key()) -> brod:partition().
partition(PartitionsCount, Key) ->
    erlang:phash2(Key) rem PartitionsCount.
