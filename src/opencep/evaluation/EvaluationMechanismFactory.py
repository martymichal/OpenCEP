from datetime import timedelta
from typing import List, Dict

from opencep.adaptive.optimizer.Optimizer import Optimizer
from opencep.adaptive.statistics.StatisticsCollector import StatisticsCollector
from opencep.base.Pattern import Pattern
from opencep.evaluation.EvaluationMechanismTypes import EvaluationMechanismTypes
from opencep.misc import DefaultConfig
from opencep.plan.multi.LocalSearchTreePlanMerger import LocalSearchTreePlanMerger
from opencep.plan.multi.local_search.LocalSearchFactory import LocalSearchParameters, TabuSearchLocalSearchParameters
from opencep.tree.evaluation.TreeEvaluationMechanismUpdateTypes import TreeEvaluationMechanismUpdateTypes
from opencep.adaptive.optimizer.OptimizerFactory import OptimizerParameters, OptimizerFactory, \
    StatisticsDeviationAwareOptimizerParameters
from opencep.adaptive.statistics.StatisticsCollectorFactory import StatisticsCollectorFactory
from opencep.plan.multi.ShareLeavesTreePlanMerger import ShareLeavesTreePlanMerger
from opencep.plan.multi.SubTreeSharingTreePlanMerger import SubTreeSharingTreePlanMerger
from opencep.plan.TreePlan import TreePlan
from opencep.plan.multi.MultiPatternTreePlanMergeApproaches import MultiPatternTreePlanMergeApproaches
from opencep.tree.PatternMatchStorage import TreeStorageParameters
from opencep.tree.evaluation.SimultaneousTreeBasedEvaluationMechanism import SimultaneousTreeBasedEvaluationMechanism
from opencep.tree.evaluation.TrivialTreeBasedEvaluationMechnism import TrivialTreeBasedEvaluationMechanism


class EvaluationMechanismParameters:
    """
    Parameters required for evaluation mechanism creation.
    """
    def __init__(self, eval_mechanism_type: EvaluationMechanismTypes = DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE,
                 optimizer_params: OptimizerParameters = OptimizerParameters()):
        self.type = eval_mechanism_type
        self.optimizer_params = optimizer_params


class TreeBasedEvaluationMechanismParameters(EvaluationMechanismParameters):
    """
    Parameters for the creation of a tree-based evaluation mechanism.
    """
    def __init__(self,
                 storage_params: TreeStorageParameters = TreeStorageParameters(),
                 optimizer_params: OptimizerParameters = StatisticsDeviationAwareOptimizerParameters(),
                 tree_update_type: TreeEvaluationMechanismUpdateTypes = DefaultConfig.DEFAULT_TREE_UPDATE_TYPE,
                 local_search_params: LocalSearchParameters = TabuSearchLocalSearchParameters()):
        super().__init__(EvaluationMechanismTypes.TREE_BASED, optimizer_params)
        self.storage_params = storage_params
        self.tree_update_type = tree_update_type
        self.local_search_params = local_search_params


class EvaluationMechanismFactory:
    """
    Creates an evaluation mechanism given its specification.
    """
    @staticmethod
    def build_eval_mechanism(eval_mechanism_params: EvaluationMechanismParameters,
                             patterns: Pattern or List[Pattern]):
        if eval_mechanism_params is None:
            eval_mechanism_params = EvaluationMechanismFactory.__create_default_eval_parameters()
        if eval_mechanism_params.type == EvaluationMechanismTypes.TREE_BASED:
            return EvaluationMechanismFactory.__create_tree_based_eval_mechanism(eval_mechanism_params, patterns)
        raise Exception("Unknown evaluation mechanism type: %s" % (eval_mechanism_params.type,))

    @staticmethod
    def __create_tree_based_eval_mechanism(eval_mechanism_params: TreeBasedEvaluationMechanismParameters,
                                           patterns: Pattern or List[Pattern]):
        """
        Instantiates a tree-based CEP evaluation mechanism according to the given configuration.
        As of this version, adaptivity only works with single-pattern CEP. It is the responsibility of the user to
        disable adaptivity in the multi-pattern mode.
        """
        if isinstance(patterns, Pattern):
            patterns = [patterns]

        optimizer_params = eval_mechanism_params.optimizer_params
        statistic_collector_params = optimizer_params.statistics_collector_params
        statistics_collector = StatisticsCollectorFactory.build_statistics_collector(statistic_collector_params,
                                                                                     patterns)
        optimizer = OptimizerFactory.build_optimizer(eval_mechanism_params.optimizer_params)
        cost_model_type = eval_mechanism_params.optimizer_params.tree_plan_params.cost_model_type
        pattern_to_tree_plan_map = {pattern: optimizer.build_initial_plan(pattern, cost_model_type)
                                    for pattern in patterns}
        runtime_statistics_collector = statistics_collector if optimizer.is_adaptivity_enabled() else None

        if len(patterns) > 1:
            # a multi-pattern case - try to merge the tree plans
            pattern_to_tree_plan_map = EvaluationMechanismFactory.__merge_tree_plans(
                pattern_to_tree_plan_map, eval_mechanism_params, optimizer)

        return EvaluationMechanismFactory.__create_tree_based_evaluation_mechanism_by_update_type(
            pattern_to_tree_plan_map, eval_mechanism_params.storage_params, runtime_statistics_collector, optimizer,
            optimizer_params.statistics_updates_time_window, eval_mechanism_params.tree_update_type)

    @staticmethod
    def __merge_tree_plans(pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                           eval_mechanism_params: TreeBasedEvaluationMechanismParameters, optimizer: Optimizer):
        """
        Merges the given tree plans of individual tree plans into a global shared structure.
        """
        tree_plan_merge_approach = eval_mechanism_params.optimizer_params.tree_plan_params.tree_plan_merge_type
        if tree_plan_merge_approach == MultiPatternTreePlanMergeApproaches.TREE_PLAN_TRIVIAL_SHARING_LEAVES:
            return ShareLeavesTreePlanMerger().merge_tree_plans(pattern_to_tree_plan_map)
        if tree_plan_merge_approach == MultiPatternTreePlanMergeApproaches.TREE_PLAN_SUBTREES_UNION:
            return SubTreeSharingTreePlanMerger().merge_tree_plans(pattern_to_tree_plan_map)
        if tree_plan_merge_approach == MultiPatternTreePlanMergeApproaches.TREE_PLAN_LOCAL_SEARCH:
            return LocalSearchTreePlanMerger().merge_tree_plans(pattern_to_tree_plan_map,
                                                                eval_mechanism_params.local_search_params,
                                                                optimizer)
        raise Exception("Unsupported multi-pattern merge algorithm %s" % (tree_plan_merge_approach,))

    @staticmethod
    def __create_default_eval_parameters():
        """
        Uses the default configuration to create evaluation mechanism parameters.
        """
        if DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE == EvaluationMechanismTypes.TREE_BASED:
            return TreeBasedEvaluationMechanismParameters()
        raise Exception("Unknown evaluation mechanism type: %s" % (DefaultConfig.DEFAULT_EVALUATION_MECHANISM_TYPE,))

    @staticmethod
    def __create_tree_based_evaluation_mechanism_by_update_type(pattern_to_tree_plan_map: Dict[Pattern, TreePlan],
                                                                storage_params: TreeStorageParameters,
                                                                statistics_collector: StatisticsCollector,
                                                                optimizer: Optimizer,
                                                                statistics_update_time_window: timedelta,
                                                                tree_update_type: TreeEvaluationMechanismUpdateTypes):
        """
        Instantiates a tree-based evaluation mechanism given all the parameters.
        """
        if tree_update_type == TreeEvaluationMechanismUpdateTypes.TRIVIAL_TREE_EVALUATION:
            return TrivialTreeBasedEvaluationMechanism(pattern_to_tree_plan_map,
                                                       storage_params,
                                                       statistics_collector,
                                                       optimizer,
                                                       statistics_update_time_window)

        if tree_update_type == TreeEvaluationMechanismUpdateTypes.SIMULTANEOUS_TREE_EVALUATION:
            return SimultaneousTreeBasedEvaluationMechanism(pattern_to_tree_plan_map,
                                                            storage_params,
                                                            statistics_collector,
                                                            optimizer,
                                                            statistics_update_time_window)
        raise Exception("Unknown evaluation mechanism type: %s" % (tree_update_type,))
