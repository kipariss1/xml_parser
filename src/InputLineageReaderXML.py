from pathlib import Path
from src.interfaces.InputLineageReader import InputLineageReader
from src.StructureElementsFactory import StructureElementsFactory
from lxml import etree


class InputLineageReaderXML(InputLineageReader):

    def __init__(self, *args, **kwargs):
        self._factory = StructureElementsFactory
        self.__root = None
        super().__init__(*args, **kwargs)

    @staticmethod
    def _check_duplicates(ret_list, new_cls):
        try:
            if len(list(filter(lambda el: el['name'] == new_cls['name'], ret_list))) == 0:
                ret_list.append(new_cls)
        except AttributeError:
            ret_list.append(new_cls)

    def _read_input_file_recursively(self, root, parent):
        ret_list = []
        for i, ch in enumerate(root, start=1):
            name = ch.tag
            class_name = name[0] + name[1:].lower() + 'Class'
            attributes_list = list(set([el.tag for el in ch]))
            attributes = {a + 'S': [] for a in attributes_list}
            lxml_attributes = {key.lower(): val for key, val in ch.attrib.items()}
            attributes = {**attributes, **lxml_attributes}
            cls = self._factory.derive_new_class(
                class_name=class_name,
                attributes=attributes,
            )
            new_cls = cls(ch, parent, name)
            self._check_duplicates(ret_list, new_cls)
            for a in attributes_list:
                new_cls[a + 'S'] = self._read_input_file_recursively(ch.findall(a), new_cls)
        return ret_list

    def _read_input(self, target_file_path: Path):
        tree = etree.parse(target_file_path)
        root = tree.getroot()
        self.__root = self._read_input_file_recursively(root, None)

    @property
    def root(self):
        return self.__root


