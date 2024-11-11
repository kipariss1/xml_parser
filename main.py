from src.InputLineageReaderXML import InputLineageReaderXML
from pathlib import Path
from collections import defaultdict
import json
from enum import Enum


class InformaticaObjectHierarchy(Enum):
    REPOSITORY = 0
    FOLDER = 1
    WORKFLOW = 2
    SESSION = 3
    MAPPING = 4
    TRANSFORMATION = 5
    TRANSFORMFIELD = 6


def tree():
    return defaultdict(tree)


def find_databases(input_xml: InputLineageReaderXML, output_file_dir: str):
    res = tree()
    folder = input_xml.root[0].FOLDERS[0]
    source_list = folder.SOURCES
    target_list = folder.TARGETS
    for instance in source_list + target_list:
        if hasattr(instance, 'SOURCEFIELDS'):
            d_tab = {col.name: {'id': col.id} for col in instance.SOURCEFIELDS}
        elif hasattr(instance, 'TARGETFIELDS'):
            d_tab = {col.name: {'id': col.id} for col in instance.TARGETFIELDS}
        res[str(instance)] = d_tab
    with open(Path(output_file_dir).resolve(), 'w') as file:
        json.dump(res, file, indent=4)


def _rec_find_informatica_objs(obj: list, json_dict: dict, level: int):
    curr_level = InformaticaObjectHierarchy(level).name
    match curr_level:
        case 'TRANSFORMFIELD':
            for el in obj:
                json_dict[el.name] = {'id': el.id}
        case 'SESSION':
            for el in obj:
                folder = el.parent.parent
                mappings = folder.get_child_attr_by_matching_property('name', to=el.mappingname, child='MAPPING')
                _rec_find_informatica_objs(mappings, json_dict[str(el)], level + 1)
        case _:
            for el in obj:
                next_level = InformaticaObjectHierarchy(level + 1).name
                _rec_find_informatica_objs(el[next_level + 'S'], json_dict[str(el)], level + 1)


def find_informatica_objs(input_xml: InputLineageReaderXML, output_file_dir: str):
    res = tree()
    _rec_find_informatica_objs(input_xml.root, res, 0)
    with open(Path(output_file_dir).resolve(), 'w') as file:
        json.dump(res, file, indent=4)


def find_lineages(path):
    xml = Path(path).resolve()
    input_xml = InputLineageReaderXML(xml)
    find_databases(input_xml, 'outs\\dbs.json')
    find_informatica_objs(input_xml, 'outs\\informatica_objs.json')


if __name__ == '__main__':
    find_lineages('input.xml')
