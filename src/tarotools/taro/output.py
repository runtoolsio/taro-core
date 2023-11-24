from enum import Enum, auto
from typing import List, Tuple, Dict


class Mode(Enum):
    HEAD = auto()
    TAIL = auto()


class InMemoryOutput:

    def __init__(self):
        self._output_lines: List[Tuple[str, bool]] = []
        self._source_ranges: Dict[str, range] = {}

    def add(self, source: str, output: str, is_error: bool):
        self._output_lines.append((output, is_error))

        if source not in self._source_ranges:
            self._source_ranges[source] = range(len(self._output_lines) - 1, len(self._output_lines))
        else:
            start = self._source_ranges[source].start
            self._source_ranges[source] = range(start, len(self._output_lines))

    def fetch(self, mode=Mode.HEAD, *, source=None, lines=0) -> List[Tuple[str, bool]]:
        if lines < 0:
            raise ValueError("Invalid argument: arg `lines` cannot be negative but was " + str(lines))

        if source is not None:
            if source_range := self._source_ranges.get(source):
                source_lines = self._output_lines[source_range.start:source_range.stop]
                if lines:
                    if mode == Mode.HEAD:
                        return source_lines[:lines]
                    elif mode == Mode.TAIL:
                        return source_lines[-lines:]
                return source_lines
            else:
                return []

        if lines:
            if mode == Mode.HEAD:
                return self._output_lines[:lines]
            elif mode == Mode.TAIL:
                return self._output_lines[-lines:]

        return self._output_lines
