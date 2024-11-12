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
        if not str(instance) in res.keys():
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


def _get_souce_and_target_type_for_connection(connection):
    source_name = 'SOURCE' if 'Definition' in connection.frominstancetype else 'TRANSFORMATION'
    source_col_name = 'SOURCEFIELD' if source_name == 'SOURCE' else 'TRANSFORMFIELD'
    target_name = 'TARGET' if 'Definition' in connection.toinstancetype else 'TRANSFORMATION'
    target_col_name = 'TARGETFIELD' if target_name == 'TARGET' else 'TRANSFORMFIELD'
    return source_name, source_col_name, target_name, target_col_name


def _find_lineages_list_for_mapping(m):
    res = []
    for con in m.CONNECTORS:
        (source_name,
         source_col_name,
         target_name,
         target_col_name) = _get_souce_and_target_type_for_connection(con)
        source_parent = con.parent.parent if source_name == 'SOURCE' else m
        source_obj = source_parent.get_child_attr_by_matching_property(
            'name', to=con.frominstance, child=source_name
        )[0]
        source_col = source_obj.get_child_attr_by_matching_property(
            'name', to=con.fromfield, child=source_col_name
        )[0]
        target_parent = con.parent.parent if target_name == 'TARGET' else m
        target_obj = target_parent.get_child_attr_by_matching_property(
            'name', to=con.toinstance, child=target_name
        )[0]
        target_col = target_obj.get_child_attr_by_matching_property(
            'name', to=con.tofield, child=target_col_name
        )[0]
        res.append((source_col.id, target_col.id))
    return res


def find_lineages_list(input_xml: InputLineageReaderXML, output_file_dir: str):
    res = []
    for r in input_xml.root:
        for f in r.FOLDERS:
            for m in f.MAPPINGS:
                res = res + _find_lineages_list_for_mapping(m)
    with open(Path(output_file_dir).resolve(), 'w') as file:
        json.dump(res, file, indent=4)


def find_lineages(path):
    xml = Path(path).resolve()
    input_xml = InputLineageReaderXML(xml)
    find_databases(input_xml, 'outs\\dbs.json')
    find_informatica_objs(input_xml, 'outs\\informatica_objs.json')
    find_lineages_list(input_xml, 'outs\\lineages.json')


if __name__ == '__main__':
    find_lineages('input.xml')
