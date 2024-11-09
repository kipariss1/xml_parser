from src.InputLineageReaderXML import InputLineageReaderXML
from pathlib import Path


def read_input(path):
    input_xml = InputLineageReaderXML(path, structure_file=Path('structure.json').resolve())
    pass


if __name__ == '__main__':
    read_input(Path('input.xml').resolve())
