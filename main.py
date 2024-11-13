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


def save_to_json(output_file_dir: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            if not __name__ == '__main__':
                return res
            with open(Path(output_file_dir).resolve(), 'w') as file:
                json.dump(res, file, indent=4)
            return res
        return wrapper
    return decorator


def _import_indexes_from_source_obj(tar, src):
    for tf in tar.TARGETFIELDS:
        sfs = src.get_child_attr_by_matching_property('name', to=tf.name, child='SOURCEFIELD')
        if len(sfs) == 0:
            continue
        tf.idx = sfs[0].idx


def _check_if_target_should_be_indexed(instance):
    source_dbs_with_same_name = [e for e in instance.parent.SOURCES if e.name == instance.name]
    if len(source_dbs_with_same_name):
        _import_indexes_from_source_obj(
            instance,
            source_dbs_with_same_name[0]
        )
        return True


@save_to_json('outs\\dbs.json')
def find_databases(input_xml: InputLineageReaderXML):
    res = tree()
    folder = input_xml.root[0].FOLDERS[0]
    source_list = folder.SOURCES
    target_list = folder.TARGETS
    attr_idx = instance_idx = 1
    for instance in source_list + target_list:
        d_db = res[instance.dbdname]
        attr = 'SOURCEFIELDS' if hasattr(instance, 'SOURCEFIELDS') else 'TARGETFIELDS'
        if hasattr(instance, 'TARGETFIELDS') and _check_if_target_should_be_indexed(instance):
            continue
        if instance.name in d_db:
            continue
        instance.idx = instance_idx
        d_tab = d_db[instance.name]
        for attr in instance[attr]:
            if attr.name in d_tab:
                continue
            d_tab[attr.name] = {'id': attr_idx}
            attr.idx = attr_idx
            attr_idx += 1
        instance_idx += 1
    return res


def _rec_find_informatica_objs(obj: list, json_dict: dict, level: int, col_cnt: int):
    curr_level = InformaticaObjectHierarchy(level).name
    for el in obj:
        if el.name in json_dict:
            continue
        match curr_level:
            case 'TRANSFORMFIELD':
                el.idx = col_cnt
                json_dict[el.name] = {'id': col_cnt}
                col_cnt += 1
            case 'SESSION':
                folder = el.parent.parent
                mappings = folder.get_child_attr_by_matching_property('name', to=el.mappingname, child='MAPPING')
                col_cnt = _rec_find_informatica_objs(mappings, json_dict[el.name], level + 1, col_cnt)
            case _:
                next_level = InformaticaObjectHierarchy(level + 1).name
                col_cnt = _rec_find_informatica_objs(el[next_level + 'S'], json_dict[el.name], level + 1, col_cnt)
    return col_cnt


@save_to_json('outs\\informatica_objs.json')
def find_informatica_objs(input_xml: InputLineageReaderXML):
    res = tree()
    _rec_find_informatica_objs(input_xml.root, res, 0, 1)
    return res


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
        res.append((source_col.idx, target_col.idx))
    return res


@save_to_json('outs\\lineages.json')
def find_lineages_list(input_xml: InputLineageReaderXML):
    res = []
    for r in input_xml.root:
        for f in r.FOLDERS:
            for m in f.MAPPINGS:
                res = res + _find_lineages_list_for_mapping(m)
    return res


def find_lineages(path):
    xml = Path(path).resolve()
    input_xml = InputLineageReaderXML(xml)
    find_databases(input_xml)
    find_informatica_objs(input_xml)
    find_lineages_list(input_xml)


if __name__ == '__main__':
    find_lineages('input.xml')
