import pytest
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock
from src.InputLineageReaderXML import InputLineageReaderXML
from main import find_databases


class TestFindDatabases:

    @pytest.fixture(autouse=True)
    def xml_base(self):
        def _xml_base(fill):
            return f"""<?xml version="1.0" encoding="Windows-1252"?>
            <!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
                <POWERMART CREATION_DATE="08/22/2020 00:43:07" REPOSITORY_VERSION="185.94">
                    <REPOSITORY NAME="INF_REP_DEV" VERSION="185" CODEPAGE="MS1252" DATABASETYPE="Oracle">
                        <FOLDER NAME="CollibraSample" GROUP="" OWNER="IDWBI" SHARED="NOTSHARED" DESCRIPTION="" PERMISSIONS="rwx---r--" UUID="dd8f69f3-5d32-40e8-bade-d623d45b5114">
                            {fill}
                        </FOLDER>
                    </REPOSITORY>
                </POWERMART>
            """
        return _xml_base

    @pytest.fixture
    def databases_xml_fill(self):
        return """
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="DimProduct" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="PRIMARY KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="PRIMARY KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="PRIMARY KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="nvarchar" FIELDNUMBER ="7" LENGTH ="0" LEVEL ="0" NAME ="SpanishProductName" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="nvarchar" FIELDNUMBER ="7" LENGTH ="0" LEVEL ="0" NAME ="SpanishProductName" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="nvarchar" FIELDNUMBER ="7" LENGTH ="0" LEVEL ="0" NAME ="SpanishProductName" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="nvarchar" FIELDNUMBER ="8" LENGTH ="0" LEVEL ="0" NAME ="FrenchProductName" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="3" LENGTH ="11" LEVEL ="0" NAME ="DueDateKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="4" LENGTH ="11" LEVEL ="0" NAME ="ShipDateKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="5" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="CustomerKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="CustomerKey" REFERENCEDTABLE ="DimCustomer" />
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="6" LENGTH ="11" LEVEL ="0" NAME ="PromotionKey" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="3" LENGTH ="11" LEVEL ="0" NAME ="DueDateKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="4" LENGTH ="11" LEVEL ="0" NAME ="ShipDateKey" NULLABLE ="NOTNULL"/>
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="5" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="CustomerKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="CustomerKey" REFERENCEDTABLE ="DimCustomer" />
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="6" LENGTH ="11" LEVEL ="0" NAME ="PromotionKey" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="FactInternetSales" DBDNAME ="Raw" OWNERNAME ="dbo" >
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="1" KEYTYPE ="NOT A KEY" NAME ="ProductKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="OrderDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="3" KEYTYPE ="NOT A KEY" NAME ="DueDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="4" KEYTYPE ="NOT A KEY" NAME ="ShipDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
        </TARGET>
        <TARGET  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="3" LENGTH ="11" LEVEL ="0" NAME ="DueDateKey" NULLABLE ="NOTNULL"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="4" LENGTH ="11" LEVEL ="0" NAME ="ShipDateKey" NULLABLE ="NOTNULL"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="5" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="CustomerKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="CustomerKey" REFERENCEDTABLE ="DimCustomer" />
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="6" LENGTH ="11" LEVEL ="0" NAME ="PromotionKey" NULLABLE ="NOTNULL"/>
        </TARGET>
        """

    @pytest.fixture(autouse=True)
    def parsed_databases_xml(self, xml_base, databases_xml_fill):
        mock_xml = ET.fromstring(xml_base(databases_xml_fill))
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_root = MagicMock()
        mock_root.getroot.return_value = mock_xml
        with patch('src.InputLineageReaderXML.etree.parse', return_value=mock_root):
            xml = InputLineageReaderXML(mock_file)
            return xml

    def test__check_duplicates(self, parsed_databases_xml):
        sources = parsed_databases_xml.root[0].FOLDERS[0].SOURCES
        targets = parsed_databases_xml.root[0].FOLDERS[0].TARGETS
        assert len(sources) == 2
        assert len(targets) == 1
        res = find_databases(parsed_databases_xml)
        assert res == {
            'DimProduct':
                {
                    'ProductKey':
                        {
                            'id': 1
                        },
                    'SpanishProductName':
                        {
                            'id': 2
                        },
                    'FrenchProductName':
                        {
                            'id': 3
                        }},
            'FactInternetSales':
                {
                    'ProductKey':
                        {
                            'id': 4
                        },
                    'OrderDateKey':
                        {
                            'id': 5
                        },
                    'DueDateKey':
                        {
                            'id': 6
                        },
                    'ShipDateKey':
                        {
                            'id': 7
                        },
                    'CustomerKey':
                        {
                            'id': 8
                        },
                    'PromotionKey':
                        {
                            'id': 9
                        }
                }
        }



