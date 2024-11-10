from src.InputLineageReaderXML import InputLineageReaderXML
from pathlib import Path
from collections import defaultdict
import json


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
    with open(Path(output_file_dir).resolve(), 'x') as file:
        json.dump(res, file, indent=4)


def find_informatica_objs(input_xml: InputLineageReaderXML, output_file_dir: str):
    pass


def find_lineages(path):
    xml = Path(path).resolve()
    structure_file = Path('structure.json').resolve()
    input_xml = InputLineageReaderXML(xml, structure_file=structure_file)
    find_databases(input_xml, 'outs\\dbs.json')
    # TODO: Find all informatica objects and transformations
    find_informatica_objs(input_xml, 'outs\\informatica_objs.json')


if __name__ == '__main__':
    find_lineages('input.xml')
