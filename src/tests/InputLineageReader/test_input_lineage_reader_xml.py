import pytest
import xml.etree.ElementTree as ET
from src.InputLineageReaderXML import InputLineageReaderXML
from unittest.mock import patch, MagicMock


class TestInputReaderXML:

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
    def simple_xml_fill(self):
        fill = """
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="DimProduct" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="PRIMARY KEY" LENGTH ="11" LEVEL ="0" NAME ="CustomerKey" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="FactInternetSales" DBDNAME ="Raw" OWNERNAME ="dbo" >
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="1" KEYTYPE ="NOT A KEY" NAME ="CustomerKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
        </TARGET>
        <MAPPING ISVALID ="YES" NAME ="m_refine_customersalesreport" OBJECTVERSION ="1" VERSIONNUMBER ="1">
            <TRANSFORMATION NAME ="SQ_FactInternetSales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="CustomerKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
            </TRANSFORMATION>
            <CONNECTOR FROMFIELD ="CustomerKey" FROMINSTANCE ="DimProduct" FROMINSTANCETYPE ="Source Definition" TOFIELD ="CustomerKey" TOINSTANCE ="SQ_FactInternetSales" TOINSTANCETYPE ="Source Qualifier"/>
            <CONNECTOR FROMFIELD ="CustomerKey" FROMINSTANCE ="SQ_FactInternetSales" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="CustomerKey" TOINSTANCE ="FactInternetSales" TOINSTANCETYPE ="Target Definition"/>
            <INSTANCE NAME ="DimProduct" TRANSFORMATION_NAME ="SQ_FactInternetSales" TRANSFORMATION_TYPE ="Source Definition" TYPE ="SOURCE"/>
            <INSTANCE NAME ="SQ_FactInternetSales" REUSABLE ="NO" TRANSFORMATION_NAME ="SQ_FactInternetSales" TRANSFORMATION_TYPE ="Source Qualifier" TYPE ="TRANSFORMATION"/>
            <INSTANCE NAME ="FactInternetSales" TRANSFORMATION_NAME ="SQ_FactInternetSales" TRANSFORMATION_TYPE ="Target Definition" TYPE ="TARGET"/>
        </MAPPING>
        <WORKFLOW ISENABLED ="YES" ISRUNNABLESERVICE ="NO" ISSERVICE ="NO" ISVALID ="YES" NAME ="collibra_sample" REUSABLE_SCHEDULER ="NO" SCHEDULERNAME ="Scheduler" SERVERNAME ="INF_INT_DEV" SERVER_DOMAINNAME ="Domain" SUSPEND_ON_ERROR ="NO" TASKS_MUST_RUN_ON_SERVER ="NO" VERSIONNUMBER ="1">
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_refine_customersalesreport" NAME ="sql" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <TASKINSTANCE NAME ="sql" TASKNAME ="sql" TASKTYPE ="Session"/>
        </WORKFLOW>
        """
        return fill

    @pytest.fixture
    def duplicate_xml_fill(self):
        return """
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="DimProduct" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="PRIMARY KEY" LENGTH ="11" LEVEL ="0" NAME ="CustomerKey" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="DimProduct" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="PRIMARY KEY" LENGTH ="11" LEVEL ="0" NAME ="CustomerKey" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="FactInternetSales" DBDNAME ="Raw" OWNERNAME ="dbo" >
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="1" KEYTYPE ="NOT A KEY" NAME ="CustomerKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
        </TARGET>
        <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="FactInternetSales" DBDNAME ="Raw" OWNERNAME ="dbo" >
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="1" KEYTYPE ="NOT A KEY" NAME ="CustomerKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
        </TARGET>
        """

    def test__parce_simple_xml(self, xml_base, simple_xml_fill, monkeypatch):
        mock_xml = ET.fromstring(xml_base(simple_xml_fill))
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_root = MagicMock()
        mock_root.getroot.return_value = mock_xml
        with patch('src.InputLineageReaderXML.etree.parse', return_value=mock_root):
            xml = InputLineageReaderXML(mock_file)
            assert xml.root[0].name == 'INF_REP_DEV'
            assert len(xml.root[0].FOLDERS) == 1
            folder = xml.root[0].FOLDERS[0]
            assert folder.name == 'CollibraSample'
            assert hasattr(folder, 'MAPPINGS')
            assert hasattr(folder, 'SOURCES')
            assert hasattr(folder, 'TARGETS')
            assert hasattr(folder, 'WORKFLOWS')
            mapping = folder.MAPPINGS[0]
            source = folder.SOURCES[0]
            target = folder.TARGETS[0]
            workflow = folder.WORKFLOWS[0]
            assert mapping.name == 'm_refine_customersalesreport'
            assert hasattr(mapping, 'CONNECTORS')
            assert hasattr(mapping, 'INSTANCES')
            assert hasattr(mapping, 'TRANSFORMATIONS')
            assert len(mapping.CONNECTORS) == 2
            assert len(mapping.INSTANCES) == 3
            assert len(mapping.TRANSFORMATIONS) == 1
            assert source.name == 'DimProduct'
            assert str(source.SOURCEFIELDS[0]) == '1. CustomerKey'
            assert target.name == 'FactInternetSales'
            assert str(target.TARGETFIELDS[0]) == '1. CustomerKey'
            assert workflow.name == 'collibra_sample'
            assert hasattr(workflow, 'SESSIONS')
            assert hasattr(workflow, 'TASKINSTANCES')
            assert str(workflow.SESSIONS[0]) == '1. sql'
            assert str(workflow.TASKINSTANCES[0]) == '1. sql'

    def test__parced_xml_doesnt_have_duplicates(self, xml_base, duplicate_xml_fill, monkeypatch):
        mock_xml = ET.fromstring(xml_base(duplicate_xml_fill))
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_root = MagicMock()
        mock_root.getroot.return_value = mock_xml
        with patch('src.InputLineageReaderXML.etree.parse', return_value=mock_root):
            xml = InputLineageReaderXML(mock_file)
            sources = xml.root[0].FOLDERS[0].SOURCES
            targets = xml.root[0].FOLDERS[0].TARGETS
            assert len(sources) == 1
            assert len(targets) == 1
            assert sources[0].id == 1
            assert targets[0].id == 2



