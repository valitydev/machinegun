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
    logger:error(
        "MG_DEBUG. resolve headless: ~p",
        [
            [
                inet:getaddrs("machinegun-ha-headless.default.svc.cluster.local", inet),
                inet:getaddrs("machinegun-ha-headless", inet),
                inet:getaddrs("machinegun-ha", inet),
                inet:getaddrs("machinegun-ha-headless.stage.empayre.com", inet)
            ]
        ]
    ),
    ReplicaCount = os:getenv("REPLICA_COUNT", "1"),
    ClusterSize = erlang:list_to_integer(ReplicaCount),
    ConnectedCount = erlang:length(erlang:nodes()),
    case is_quorum(ClusterSize, ConnectedCount) of
        true ->
            {passing, []};
        false ->
            Reason =
                <<"no quorum. cluster size: ", (erlang:list_to_binary(ReplicaCount))/binary, ", connected: ",
                    (erlang:integer_to_binary(ConnectedCount))/binary>>,
            {critical, Reason}
    end.

%% Internal functions

-spec is_quorum(non_neg_integer(), integer()) -> boolean().
is_quorum(1, _) ->
    true;
is_quorum(ClusterSize, ConnectedCount) ->
    ConnectedCount >= ClusterSize div 2.
