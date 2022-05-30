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

-module(machinegun_namespace_sup).

-type options() :: [mg_core_events_machine:options()].

-export_type([options/0]).

-export([child_spec/2]).
-export([start_link/2]).

%%

-spec child_spec(options(), _ChildID) -> supervisor:child_spec().
child_spec(Options, ChildID) ->
    #{
        id => ChildID,
        start => {?MODULE, start_link, [Options, ChildID]},
        restart => permanent,
        type => supervisor
    }.

-spec start_link(options(), _ChildID) -> mg_core_utils:gen_start_ret().
start_link(Options, ChildID) ->
    StartRet = mg_core_utils_supervisor_wrapper:start_link(
        #{strategy => simple_one_for_one},
        [
            #{
                id => ChildID,
                start => {mg_core_events_machine, start_link, []},
                restart => permanent,
                type => supervisor
            }
        ]
    ),
    case StartRet of
        {ok, SupPid} ->
            start_namespace_children(SupPid, Options);
        Other ->
            Other
    end.

-spec start_namespace_children(pid(), options()) -> mg_core_utils:gen_start_ret().
start_namespace_children(SupPid, []) ->
    {ok, SupPid};
start_namespace_children(SupPid, [NsOptions | Rest]) ->
    case supervisor:start_child(SupPid, [NsOptions]) of
        {ok, _} -> start_namespace_children(SupPid, Rest);
        {ok, _, _} -> start_namespace_children(SupPid, Rest);
        {error, Reason} -> {error, {start_child, Reason}}
    end.
