from typing import List, Tuple


class InMemoryOutput:

    def __init__(self):
        self.output_lines: List[Tuple[str, bool]] = []

    def fetch(self) -> List[Tuple[str, bool]]:
        return self.output_lines
