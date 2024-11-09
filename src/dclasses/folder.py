from dataclasses import dataclass, field
from typing import List
from lxml import etree


@dataclass
class Folder:
    uuid: str
    mappings: List[etree.Element] = field(default_factory=list)
    sources: List[etree.Element] = field(default_factory=list)
    targets: List[etree.Element] = field(default_factory=list)
    workflows: List[etree.Element] = field(default_factory=list)

    def __getitem__(self, key):
        return getattr(self, key)

    def __setitem__(self, key, value):
        setattr(self, key, value)
