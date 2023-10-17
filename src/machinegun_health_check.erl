-module(machinegun_health_check).

-export([consuela/0]).
-export([global/0]).

-spec consuela() -> {erl_health:status(), erl_health:details()}.
consuela() ->
    ReplicaCount = os:getenv("REPLICA_COUNT", "1"),
    logger:info("MG_DEBUG. Cluster size: ~p, nodes: ~p, node: ~p", [ReplicaCount, nodes(), node()]),
    case consuela:test() of
        ok -> {passing, []};
        {error, Reason} -> {critical, genlib:format(Reason)}
    end.

-spec global() -> {erl_health:status(), erl_health:details()}.
global() ->
    ReplicaCount = os:getenv("REPLICA_COUNT", "1"),
    ClusterSize = erlang:list_to_integer(ReplicaCount),
    ConnectedCount = erlang:length(erlang:nodes()),
    logger:info("MG_DEBUG. Cluster size: ~p, nodes: ~p, node: ~p", [ClusterSize, nodes(), node()]),
    case is_quorum(ClusterSize, ConnectedCount) of
        true ->
            {passing, []};
        false ->
            {critical,
                <<"no quorum. cluster size: ", (erlang:list_to_binary(ReplicaCount))/binary, ", connected: ",
                    (erlang:integer_to_binary(ConnectedCount))/binary>>}
    end.

%% Internal functions

-spec is_quorum(non_neg_integer(), integer()) -> boolean().
is_quorum(1, _) ->
    true;
is_quorum(ClusterSize, ConnectedCount) ->
    ConnectedCount >= ClusterSize div 2.
