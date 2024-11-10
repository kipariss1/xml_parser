from src.InputLineageReaderXML import InputLineageReaderXML
from pathlib import Path
from collections import defaultdict
import json
from typing import Type
from src.StructureElementsFactory import BaseStructureClass
from enum import Enum


class InformaticaObjectHierarchy(Enum):
    WORKFLOW = 0
    SESSION = 1
    MAPPING = 2
    TRANSFORMATION = 3


def find_databases(input_xml: InputLineageReaderXML, output_file_dir: str):
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


def _rec_find_informatica_objs(obj: Type[BaseStructureClass], json_dict):
    pass


def find_informatica_objs(input_xml: InputLineageReaderXML, output_file_dir: str):
    # TODO: do it recursively here
    res = {}
    folder = input_xml.root[0].FOLDERS[0]
    for wf in folder.WORKFLOWS:
        d_wf = {}
        _rec_find_informatica_objs(wf, d_wf)


def find_lineages(path):
    xml = Path(path).resolve()
    structure_file = Path('structure.json').resolve()
    input_xml = InputLineageReaderXML(xml, structure_file=structure_file)
    find_databases(input_xml, 'outs\\dbs.json')
    # TODO: Find all informatica objects and transformations
    find_informatica_objs(input_xml, 'outs\\informatica_objs.json')


if __name__ == '__main__':
    find_lineages('input.xml')
