from typing import Optional

from taro.jobs.job import InstanceMatchingCriteria


def instance_matching_criteria(args, def_id_match_strategy) -> Optional[InstanceMatchingCriteria]:
    if args.instances:
        return InstanceMatchingCriteria(args.instances, id_match_strategy=def_id_match_strategy)
    else:
        return None
