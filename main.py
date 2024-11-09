from InputLineageReaderXML import InputLineageReaderXML
from pathlib import Path


def read_input(path):
    input_xml = InputLineageReaderXML(path)
    just_folder = list(input_xml.folders[0].values())[0]
    pass


if __name__ == '__main__':
    read_input(Path('input.xml').resolve())
