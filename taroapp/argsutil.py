from typing import Optional

from taro.jobs.job import IDMatchingCriteria, InstanceMatchingCriteria


def id_matching_criteria(args, def_id_match_strategy) -> Optional[IDMatchingCriteria]:
    """
    :param args: cli args
    :param def_id_match_strategy: id match strategy used when not overridden by args
    :return: instance of ID match criteria or None when args has no criteria specified
    """
    if args.instances:
        return IDMatchingCriteria(args.instances, def_id_match_strategy)
    else:
        return None


def instance_matching_criteria(args, def_id_match_strategy) -> Optional[InstanceMatchingCriteria]:
    if args.instances:
        return InstanceMatchingCriteria(IDMatchingCriteria(args.instances, def_id_match_strategy))
    else:
        return None
