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


def tree():
    return defaultdict(tree)


def find_databases(input_xml: InputLineageReaderXML, output_file_dir: str):
    # TODO: redo without the lists
    res = defaultdict(list)
    folder = input_xml.root[0].FOLDERS[0]
    source_list = folder.SOURCES
    target_list = folder.TARGETS
    for instance in source_list + target_list:
        table = {}
        res[instance.dbdname].append(table)
        d_tab = {}
        if hasattr(instance, 'SOURCEFIELDS'):
            d_tab = {col.name: {'id': int(col.fieldnumber)} for col in instance.SOURCEFIELDS}
        elif hasattr(instance, 'TARGETFIELDS'):
            d_tab = {col.name: {'id': int(col.fieldnumber)} for col in instance.TARGETFIELDS}
        table[instance.name] = d_tab
    with open(Path(output_file_dir).resolve(), 'w') as file:
        json.dump(res, file, indent=4)


def _get_mapping_for_session(sess):
    mapping_name = sess.mappingname
    while type(sess.parent).__name__ != 'RepositoryClass':
        sess = sess.parent
    folder = sess
    for map in folder.MAPPINGS:
        if map.name == mapping_name:
            return map
    return None


def _rec_find_informatica_objs(obj: list, json_dict: dict, level: int):
    curr_level = InformaticaObjectHierarchy(level).name
    if curr_level == 'TRANSFORMATION':
        # TODO: find connectors for transformation and find id's from connectors
        dummy_val = {'col1': {'id': 3}, 'col2': {'id': 4}}
        for el in obj:
            json_dict[el.name] = dummy_val
        return
    if curr_level == 'SESSION':
        for el in obj:
            mapping = _get_mapping_for_session(el)
            _rec_find_informatica_objs([mapping], json_dict[el.name], level + 1)
        return
    for el in obj:
        next_level = InformaticaObjectHierarchy(level + 1).name
        _rec_find_informatica_objs(el[next_level + 'S'], json_dict[el.name], level + 1)


def find_informatica_objs(input_xml: InputLineageReaderXML, output_file_dir: str):
    res = tree()
    _rec_find_informatica_objs(input_xml.root, res, 0)
    with open(Path(output_file_dir).resolve(), 'w') as file:
        json.dump(res, file, indent=4)


def find_lineages(path):
    xml = Path(path).resolve()
    structure_file = Path('structure.json').resolve()
    input_xml = InputLineageReaderXML(xml, structure_file=structure_file)
    find_databases(input_xml, 'outs\\dbs.json')
    find_informatica_objs(input_xml, 'outs\\informatica_objs.json')


if __name__ == '__main__':
    find_lineages('input.xml')
