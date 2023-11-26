"""
This module defines the instance part of the job framework and is built on top of the execution framework defined
in the execution module.

The main parts are:
1. The job instance abstraction: An interface of job instance
2. `JobInst` class: An immutable snapshot of a job instance
3. Job instance observers

Note: See the `runner` module for the default job instance implementation
TODO:
1. Add `labels`
"""

from dataclasses import dataclass
from typing import Dict, Any, Optional


@dataclass
class Warn:
    """
    This class represents a warning.

    Attributes:
        name (str): Name is used to identify the type of the warning.
        params (Optional[Dict[str, Any]]): Arbitrary parameters related to the warning.
    """
    name: str
    params: Optional[Dict[str, Any]] = None


@dataclass
class WarnEventCtx:
    """
    A class representing information related to a warning event.

    Attributes:
        warning (Warn): The warning which initiated the event.
        count (int): The total number of warnings with the same name associated with the instance.
    """
    warning: Warn
    count: int


