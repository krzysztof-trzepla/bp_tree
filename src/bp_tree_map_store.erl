%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree_map_store).
-author("Krzysztof Trzepla").

-behaviour(bp_tree_store).

%% bp_tree_store callbacks
-export([init/1, terminate/1]).
-export([set_root/2, get_root/1, get_node/2, save_node/3, delete_node/2]).

-type state() :: maps:map([{'$root', bp_tree_node:id()} |
                           {bp_tree_node:id(), bp_tree:node()}]).

%%====================================================================
%% bp_tree_store callbacks
%%====================================================================

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec init(bp_tree_store:args()) -> state().
init(_Args) ->
    #{}.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec set_root(bp_tree_node:id(), state()) ->
    {ok | {error, Reason :: term()}, state()}.
set_root(NodeId, State) ->
    {ok, maps:put('$root', NodeId, State)}.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec get_root(state()) ->
    {{ok, bp_tree_node:id()} | {error, Reason :: term()}, state()}.
get_root(State) ->
    case maps:find('$root', State) of
        {ok, NodeId} -> {{ok, NodeId}, State};
        error -> {{error, not_found}, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec get_node(bp_tree_node:id(), state()) ->
    {{ok, bp_tree:tree_node()} | {error, Reason :: term()}, state()}.
get_node(NodeId, State) ->
    case maps:find(NodeId, State) of
        {ok, Node} -> {{ok, Node}, State};
        error -> {{error, not_found}, State}
    end.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec save_node(bp_tree_node:id(), bp_tree:tree_node(), state()) ->
    {ok | {error, Reason :: term()}, state()}.
save_node(NodeId, Node, State) ->
    {ok, maps:put(NodeId, Node, State)}.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec delete_node(bp_tree_node:id(), state()) ->
    {ok | {error, Reason :: term()}, state()}.
delete_node(NodeId, State) ->
    {ok, maps:remove(NodeId, State)}.

%%--------------------------------------------------------------------
%% @doc
%% @todo write me!
%% @end
%%--------------------------------------------------------------------
-spec terminate(state()) -> ok.
terminate(_State) ->
    ok.
