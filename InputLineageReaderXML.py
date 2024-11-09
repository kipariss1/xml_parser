from pathlib import Path
from interfaces.InputLineageReader import InputLineageReader
from lxml import etree
from collections import defaultdict
from dclasses.folder import Folder
from typing import Union, List


class InputLineageReaderXML(InputLineageReader):

    def __init__(self, *args, **kwargs):
        self.categories = ['MAPPING', 'SOURCE', 'TARGET', 'WORKFLOW']
        self._levels = ['REPOSITORY', 'FOLDER', self.categories]
        self.__folders = []
        super().__init__(*args, **kwargs)

    def _read_folder(self, el: etree.Element):
        curr_folder = Folder()
        for category in self.categories:
            curr_folder[category.lower() + 's'] = el.findall(category)
        return curr_folder

    def _recursively_read_xml(
            self,
            el: etree.Element,
            level: int,
            structure_el: defaultdict,
            parent: Union[defaultdict, None],
            parent_name: str
    ):
        if isinstance(self._levels[level], list):
            f = self._read_folder(el)
            parent[parent_name] = f
            self.__folders.append(parent)
            return
        level_name = self._levels[level]
        for e in el.findall(level_name):
            self._recursively_read_xml(e, level + 1, structure_el[e.attrib['NAME']], structure_el, e.attrib['NAME'])

    def _read_input(self, target_file_path: Path):
        tree = etree.parse(target_file_path)
        root = tree.getroot()
        self._recursively_read_xml(root, 0, self.structure, None, '')

    @property
    def folders(self) -> List[defaultdict]:
        return self.__folders


