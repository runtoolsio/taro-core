from typing import List, Tuple, Dict


class InMemoryOutput:

    def __init__(self):
        self.output_lines: List[Tuple[str, bool]] = []
        self.source_ranges: Dict[str, range] = {}

    def add(self, source: str, output: str, is_error: bool):
        self.output_lines.append((output, is_error))

        if source not in self.source_ranges:
            self.source_ranges[source] = range(len(self.output_lines) - 1, len(self.output_lines))
        else:
            start = self.source_ranges[source].start
            self.source_ranges[source] = range(start, len(self.output_lines))

    def fetch(self, source=None) -> List[Tuple[str, bool]]:
        if source is not None:
            if source_range := self.source_ranges.get(source):
                return self.output_lines[source_range.start:source_range.stop]
            else:
                return []

        return self.output_lines
