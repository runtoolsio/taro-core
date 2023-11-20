from typing import List, Tuple, Dict


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

    def fetch(self, source=None) -> List[Tuple[str, bool]]:
        if source is not None:
            if source_range := self._source_ranges.get(source):
                return self._output_lines[source_range.start:source_range.stop]
            else:
                return []

        return self._output_lines
