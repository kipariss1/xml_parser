from pathlib import Path
from abc import ABC, abstractmethod
from collections import defaultdict


class InputLineageReader(ABC):

    def __init__(self, input: Path):
        self._input_file = input
        self.structure = defaultdict(self._tree)
        if not self._input_file.is_file():
            raise FileNotFoundError('input xml file is not found')
        self._read_input(self._input_file)

    def _tree(self):
        return defaultdict(self._tree)

    @abstractmethod
    def _read_input(self, target_file_path: Path):
        raise NotImplemented

