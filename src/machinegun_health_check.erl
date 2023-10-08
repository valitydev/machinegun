-module(machinegun_health_check).

-export([consuela/0]).
-export([global/0]).

-spec consuela() -> {erl_health:status(), erl_health:details()}.
consuela() ->
    case consuela:test() of
        ok -> {passing, []};
        {error, Reason} -> {critical, genlib:format(Reason)}
    end.

-spec global() -> {erl_health:status(), erl_health:details()}.
global() ->
    ClusterSize = erlang:list_to_integer(os:getenv("REPLICA_COUNT", "1")),
    case is_quorum(ClusterSize) of
        true -> {passing, []};
        false -> {critical, <<"no quorum">>}
    end.

%% Internal functions

is_quorum(1) ->
    true;
is_quorum(ClusterSize) ->
    ConnectedCount = erlang:length(erlang:nodes()),
    ConnectedCount >= ClusterSize div 2.
