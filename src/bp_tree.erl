%%%-------------------------------------------------------------------
%%% @author Krzysztof Trzepla
%%% @copyright (C) 2017: Krzysztof Trzepla
%%% This software is released under the MIT license cited in 'LICENSE.md'.
%%% @end
%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(bp_tree).
-author("Krzysztof Trzepla").

-include("bp_tree.hrl").

%% API exports
-export([]).

-type tree() :: #bp_tree{}.
-type tree_node() :: #bp_tree_node{}.
-type option() :: {degree, pos_integer()} |
                  {store_module, module()} |
                  {store_args, bp_tree_store:args()}.

-export_type([tree/0, tree_node/0, option/0]).

%%====================================================================
%% API functions
%%====================================================================

%%====================================================================
%% Internal functions
%%====================================================================
