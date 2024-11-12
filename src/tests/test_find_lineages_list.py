import pytest
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock
from src.InputLineageReaderXML import InputLineageReaderXML
from main import find_lineages_list, find_databases, find_informatica_objs


class TestFindLineages:

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
    def base_find_lineages_xml(self):
        return """
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="CustomerProductSales" DBDNAME ="Refined" OWNERNAME ="dbo">
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="OrderDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="ProductKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
        </TARGET>
        <MAPPING ISVALID ="YES" NAME ="m_refine_customersalesreport" OBJECTVERSION ="1" VERSIONNUMBER ="1">
            <TRANSFORMATION NAME ="SQ_FactInternetSales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
            </TRANSFORMATION>
            <INSTANCE NAME ="SQ_FactInternetSales" REUSABLE ="NO" TRANSFORMATION_NAME ="SQ_FactInternetSales" TRANSFORMATION_TYPE ="Source Qualifier" TYPE ="TRANSFORMATION">
                <ASSOCIATED_SOURCE_INSTANCE NAME ="FactInternetSales"/>
            </INSTANCE>
            <INSTANCE NAME ="CustomerProductSales" TRANSFORMATION_NAME ="CustomerProductSales" TRANSFORMATION_TYPE ="Target Definition" TYPE ="TARGET"/>
            <INSTANCE DBDNAME ="Raw" NAME ="FactInternetSales" TRANSFORMATION_NAME ="FactInternetSales" TRANSFORMATION_TYPE ="Source Definition" TYPE ="SOURCE"/>
            <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="FactInternetSales" FROMINSTANCETYPE ="Source Definition" TOFIELD ="ProductKey" TOINSTANCE ="SQ_FactInternetSales" TOINSTANCETYPE ="Source Qualifier"/>
            <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="FactInternetSales" FROMINSTANCETYPE ="Source Definition" TOFIELD ="OrderDateKey" TOINSTANCE ="SQ_FactInternetSales" TOINSTANCETYPE ="Source Qualifier"/>
            <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="SQ_FactInternetSales" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="OrderDateKey" TOINSTANCE ="CustomerProductSales" TOINSTANCETYPE ="Target Definition"/>
            <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="SQ_FactInternetSales" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="ProductKey" TOINSTANCE ="CustomerProductSales" TOINSTANCETYPE ="Target Definition"/>
        </MAPPING>
        <WORKFLOW ISENABLED ="YES" ISRUNNABLESERVICE ="NO" ISSERVICE ="NO" ISVALID ="YES" NAME ="collibra_sample" REUSABLE_SCHEDULER ="NO" SCHEDULERNAME ="Scheduler" SERVERNAME ="INF_INT_DEV" SERVER_DOMAINNAME ="Domain" SUSPEND_ON_ERROR ="NO" TASKS_MUST_RUN_ON_SERVER ="NO" VERSIONNUMBER ="1">
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_orderentry_internetsales" NAME ="oracle" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_refine_customersalesreport" NAME ="sql" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <TASKINSTANCE NAME ="oracle" TASKNAME ="oracle" TASKTYPE ="Session"/>
            <TASKINSTANCE NAME ="sql" TASKNAME ="sql" TASKTYPE ="Session"/>
        </WORKFLOW>
        """

    @pytest.fixture
    def harder_find_lineages_xml(self):
        return """
            <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
                <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
                <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
            </SOURCE>
            <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="CustomerProductSales" DBDNAME ="Refined" OWNERNAME ="dbo">
                <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="OrderDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
                <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="ProductKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            </TARGET>
            <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales2" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
                <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
                <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
            </SOURCE>
            <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="CustomerProductSales2" DBDNAME ="Refined" OWNERNAME ="dbo">
                <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="OrderDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
                <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="ProductKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            </TARGET>
            <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales3" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
                <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
                <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
            </SOURCE>
            <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="CustomerProductSales3" DBDNAME ="Refined" OWNERNAME ="dbo">
                <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="OrderDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
                <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="ProductKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            </TARGET>
            <MAPPING ISVALID ="YES" NAME ="m_refine_customersalesreport" OBJECTVERSION ="1" VERSIONNUMBER ="1">
                <TRANSFORMATION NAME ="SQ_FactInternetSales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                </TRANSFORMATION>
                <TRANSFORMATION NAME ="SQ_FactInternetSales2" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                </TRANSFORMATION>
                <TRANSFORMATION NAME ="SQ_FactInternetSales3" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                    <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                </TRANSFORMATION>
                <INSTANCE NAME ="SQ_FactInternetSales" REUSABLE ="NO" TRANSFORMATION_NAME ="SQ_FactInternetSales" TRANSFORMATION_TYPE ="Source Qualifier" TYPE ="TRANSFORMATION">
                    <ASSOCIATED_SOURCE_INSTANCE NAME ="FactInternetSales"/>
                </INSTANCE>
                <INSTANCE NAME ="SQ_FactInternetSales2" REUSABLE ="NO" TRANSFORMATION_NAME ="SQ_FactInternetSales2" TRANSFORMATION_TYPE ="Source Qualifier" TYPE ="TRANSFORMATION">
                    <ASSOCIATED_SOURCE_INSTANCE NAME ="FactInternetSales2"/>
                </INSTANCE>
                <INSTANCE NAME ="SQ_FactInternetSales3" REUSABLE ="NO" TRANSFORMATION_NAME ="SQ_FactInternetSales3" TRANSFORMATION_TYPE ="Source Qualifier" TYPE ="TRANSFORMATION">
                    <ASSOCIATED_SOURCE_INSTANCE NAME ="FactInternetSales3"/>
                </INSTANCE>
                <INSTANCE NAME ="CustomerProductSales" TRANSFORMATION_NAME ="CustomerProductSales" TRANSFORMATION_TYPE ="Target Definition" TYPE ="TARGET"/>
                <INSTANCE DBDNAME ="Raw" NAME ="FactInternetSales" TRANSFORMATION_NAME ="FactInternetSales" TRANSFORMATION_TYPE ="Source Definition" TYPE ="SOURCE"/>
                <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="FactInternetSales" FROMINSTANCETYPE ="Source Definition" TOFIELD ="ProductKey" TOINSTANCE ="SQ_FactInternetSales" TOINSTANCETYPE ="Source Qualifier"/>
                <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="FactInternetSales" FROMINSTANCETYPE ="Source Definition" TOFIELD ="OrderDateKey" TOINSTANCE ="SQ_FactInternetSales" TOINSTANCETYPE ="Source Qualifier"/>
                <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="SQ_FactInternetSales" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="OrderDateKey" TOINSTANCE ="CustomerProductSales" TOINSTANCETYPE ="Target Definition"/>
                <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="SQ_FactInternetSales" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="ProductKey" TOINSTANCE ="CustomerProductSales" TOINSTANCETYPE ="Target Definition"/>
                <INSTANCE NAME ="CustomerProductSales2" TRANSFORMATION_NAME ="CustomerProductSales2" TRANSFORMATION_TYPE ="Target Definition" TYPE ="TARGET"/>
                <INSTANCE DBDNAME ="Raw" NAME ="FactInternetSales2" TRANSFORMATION_NAME ="FactInternetSales2" TRANSFORMATION_TYPE ="Source Definition" TYPE ="SOURCE"/>
                <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="FactInternetSales2" FROMINSTANCETYPE ="Source Definition" TOFIELD ="ProductKey" TOINSTANCE ="SQ_FactInternetSales2" TOINSTANCETYPE ="Source Qualifier"/>
                <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="FactInternetSales2" FROMINSTANCETYPE ="Source Definition" TOFIELD ="OrderDateKey" TOINSTANCE ="SQ_FactInternetSales2" TOINSTANCETYPE ="Source Qualifier"/>
                <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="SQ_FactInternetSales2" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="OrderDateKey" TOINSTANCE ="CustomerProductSales2" TOINSTANCETYPE ="Target Definition"/>
                <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="SQ_FactInternetSales2" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="ProductKey" TOINSTANCE ="CustomerProductSales2" TOINSTANCETYPE ="Target Definition"/>
                <INSTANCE NAME ="CustomerProductSales3" TRANSFORMATION_NAME ="CustomerProductSales3" TRANSFORMATION_TYPE ="Target Definition" TYPE ="TARGET"/>
                <INSTANCE DBDNAME ="Raw" NAME ="FactInternetSales3" TRANSFORMATION_NAME ="FactInternetSales3" TRANSFORMATION_TYPE ="Source Definition" TYPE ="SOURCE"/>
                <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="FactInternetSales3" FROMINSTANCETYPE ="Source Definition" TOFIELD ="ProductKey" TOINSTANCE ="SQ_FactInternetSales3" TOINSTANCETYPE ="Source Qualifier"/>
                <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="FactInternetSales3" FROMINSTANCETYPE ="Source Definition" TOFIELD ="OrderDateKey" TOINSTANCE ="SQ_FactInternetSales3" TOINSTANCETYPE ="Source Qualifier"/>
                <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="SQ_FactInternetSales3" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="OrderDateKey" TOINSTANCE ="CustomerProductSales3" TOINSTANCETYPE ="Target Definition"/>
                <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="SQ_FactInternetSales3" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="ProductKey" TOINSTANCE ="CustomerProductSales3" TOINSTANCETYPE ="Target Definition"/>
            </MAPPING>
            <WORKFLOW ISENABLED ="YES" ISRUNNABLESERVICE ="NO" ISSERVICE ="NO" ISVALID ="YES" NAME ="collibra_sample" REUSABLE_SCHEDULER ="NO" SCHEDULERNAME ="Scheduler" SERVERNAME ="INF_INT_DEV" SERVER_DOMAINNAME ="Domain" SUSPEND_ON_ERROR ="NO" TASKS_MUST_RUN_ON_SERVER ="NO" VERSIONNUMBER ="1">
                <SESSION ISVALID ="YES" MAPPINGNAME ="m_orderentry_internetsales" NAME ="oracle" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
                </SESSION>
                <SESSION ISVALID ="YES" MAPPINGNAME ="m_refine_customersalesreport" NAME ="sql" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
                </SESSION>
                <TASKINSTANCE NAME ="oracle" TASKNAME ="oracle" TASKTYPE ="Session"/>
                <TASKINSTANCE NAME ="sql" TASKNAME ="sql" TASKTYPE ="Session"/>
            </WORKFLOW>
            """

    @pytest.fixture
    def target_equialent_to_source_xml(self):
        return """
        <SOURCE  DATABASETYPE ="Microsoft SQL Server" DBDNAME ="Raw" NAME ="FactInternetSales" OBJECTVERSION ="1" OWNERNAME ="dbo" VERSIONNUMBER ="1">
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="1" FIELDPROPERTY ="0" FIELDTYPE ="ELEMITEM" HIDDEN ="NO" KEYTYPE ="FOREIGN KEY" LENGTH ="11" LEVEL ="0" NAME ="ProductKey" NULLABLE ="NOTNULL"   REFERENCEDDBD ="Raw" REFERENCEDFIELD ="ProductKey" REFERENCEDTABLE ="DimProduct" />
            <SOURCEFIELD  DATATYPE ="int" FIELDNUMBER ="2" LENGTH ="11" LEVEL ="0" NAME ="OrderDateKey" NULLABLE ="NOTNULL"/>
        </SOURCE>
        <TARGET  CONSTRAINT ="" DATABASETYPE ="Microsoft SQL Server" NAME ="FactInternetSales" DBDNAME ="Refined" OWNERNAME ="dbo">
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="ProductKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
            <TARGETFIELD  DATATYPE ="int" FIELDNUMBER ="2" KEYTYPE ="NOT A KEY" NAME ="OrderDateKey" NULLABLE ="NOTNULL" PRECISION ="10" SCALE ="0"/>
        </TARGET>
        <MAPPING ISVALID ="YES" NAME ="m_refine_customersalesreport" OBJECTVERSION ="1" VERSIONNUMBER ="1">
            <TRANSFORMATION NAME ="SQ_FactInternetSales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
            </TRANSFORMATION>
            <INSTANCE NAME ="SQ_FactInternetSales" REUSABLE ="NO" TRANSFORMATION_NAME ="SQ_FactInternetSales" TRANSFORMATION_TYPE ="Source Qualifier" TYPE ="TRANSFORMATION">
                <ASSOCIATED_SOURCE_INSTANCE NAME ="FactInternetSales"/>
            </INSTANCE>
            <INSTANCE NAME ="CustomerProductSales" TRANSFORMATION_NAME ="CustomerProductSales" TRANSFORMATION_TYPE ="Target Definition" TYPE ="TARGET"/>
            <INSTANCE DBDNAME ="Raw" NAME ="FactInternetSales" TRANSFORMATION_NAME ="FactInternetSales" TRANSFORMATION_TYPE ="Source Definition" TYPE ="SOURCE"/>
            <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="FactInternetSales" FROMINSTANCETYPE ="Source Definition" TOFIELD ="ProductKey" TOINSTANCE ="SQ_FactInternetSales" TOINSTANCETYPE ="Source Qualifier"/>
            <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="FactInternetSales" FROMINSTANCETYPE ="Source Definition" TOFIELD ="OrderDateKey" TOINSTANCE ="SQ_FactInternetSales" TOINSTANCETYPE ="Source Qualifier"/>
            <CONNECTOR FROMFIELD ="OrderDateKey" FROMINSTANCE ="SQ_FactInternetSales" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="ProductKey" TOINSTANCE ="FactInternetSales" TOINSTANCETYPE ="Target Definition"/>
            <CONNECTOR FROMFIELD ="ProductKey" FROMINSTANCE ="SQ_FactInternetSales" FROMINSTANCETYPE ="Source Qualifier" TOFIELD ="OrderDateKey" TOINSTANCE ="FactInternetSales" TOINSTANCETYPE ="Target Definition"/>
        </MAPPING>
        <WORKFLOW ISENABLED ="YES" ISRUNNABLESERVICE ="NO" ISSERVICE ="NO" ISVALID ="YES" NAME ="collibra_sample" REUSABLE_SCHEDULER ="NO" SCHEDULERNAME ="Scheduler" SERVERNAME ="INF_INT_DEV" SERVER_DOMAINNAME ="Domain" SUSPEND_ON_ERROR ="NO" TASKS_MUST_RUN_ON_SERVER ="NO" VERSIONNUMBER ="1">
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_orderentry_internetsales" NAME ="oracle" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_refine_customersalesreport" NAME ="sql" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <TASKINSTANCE NAME ="oracle" TASKNAME ="oracle" TASKTYPE ="Session"/>
            <TASKINSTANCE NAME ="sql" TASKNAME ="sql" TASKTYPE ="Session"/>
        </WORKFLOW>
                """

    def test__find_basic_lineages_xml(self, xml_base, base_find_lineages_xml):
        mock_xml = ET.fromstring(xml_base(base_find_lineages_xml))
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_root = MagicMock()
        mock_root.getroot.return_value = mock_xml
        with patch('src.InputLineageReaderXML.etree.parse', return_value=mock_root):
            xml = InputLineageReaderXML(mock_file)
            find_databases(xml)
            find_informatica_objs(xml)
            res = find_lineages_list(xml)
            assert res == [(1, 1), (2, 2), (2, 3), (1, 4)]

    def test__find_harder_lineages_xml(self, xml_base, harder_find_lineages_xml):
        mock_xml = ET.fromstring(xml_base(harder_find_lineages_xml))
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_root = MagicMock()
        mock_root.getroot.return_value = mock_xml
        with patch('src.InputLineageReaderXML.etree.parse', return_value=mock_root):
            xml = InputLineageReaderXML(mock_file)
            find_databases(xml)
            find_informatica_objs(xml)
            res = find_lineages_list(xml)
            assert res == [(1, 1), (2, 2), (2, 7), (1, 8), (3, 3), (4, 4), (4, 9), (3, 10), (5, 5), (6, 6), (6, 11), (5, 12)]

    def test__reference_target_equialent_to_source(self, xml_base, target_equialent_to_source_xml):
        mock_xml = ET.fromstring(xml_base(target_equialent_to_source_xml))
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_root = MagicMock()
        mock_root.getroot.return_value = mock_xml
        with patch('src.InputLineageReaderXML.etree.parse', return_value=mock_root):
            xml = InputLineageReaderXML(mock_file)
            find_databases(xml)
            find_informatica_objs(xml)
            res = find_lineages_list(xml)
            assert res == [(1, 1), (2, 2), (2, 1), (1, 2)]