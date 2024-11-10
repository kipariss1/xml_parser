from pathlib import Path
from src.interfaces.InputLineageReader import InputLineageReader
from src.StructureElementsFactory import StructureElementsFactory
from lxml import etree
import json


class InputLineageReaderXML(InputLineageReader):

    def __init__(self, *args, structure_file, **kwargs):
        with open(structure_file) as file:
            self._structure = json.load(file)
        self._factory = StructureElementsFactory
        self.__root = None
        super().__init__(*args, **kwargs)

    def _read_input_file_recursively(self, lxml_element, structure) -> list:
        if isinstance(structure, str):
            return [
                self._factory.derive_and_init(structure[0] + structure[1:].lower() + 'Class', dict(el.attrib), el)
                for el in lxml_element.findall(structure)
            ]
        key, value = next(iter(structure.items()))
        ret_list = []
        class_name = key[0] + key[1:].lower() + 'Class'
        attributes_list = [list(el.keys())[0] if isinstance(el, dict) else el for el in value]
        attributes = {}
        for el in value:
            if isinstance(el, dict):
                attributes[list(el.keys())[0] + 'S'] = []
            else:
                attributes[el + 'S'] = []
        cls = self._factory.derive_new_class(
            class_name=class_name,
            attributes=attributes,
        )
        for e in lxml_element.findall(key):
            new_cls = cls()
            new_cls.lxml_element = e
            self._factory.add_attributes(
                new_cls,
                {key.lower(): val for key, val in e.attrib.items()}
            )
            # TODO: add here to new_cls already existing attributes from xml
            ret_list.append(new_cls)
            for idx, attr in enumerate(attributes_list):
                new_cls[attr + 'S'] = self._read_input_file_recursively(e, value[idx])
        return ret_list

    def _read_input(self, target_file_path: Path):
        tree = etree.parse(target_file_path)
        root = tree.getroot()
        self.__root = self._read_input_file_recursively(root, self._structure)

    @property
    def root(self):
        return self.__root


