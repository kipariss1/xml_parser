import pytest
from src.InputLineageReaderXML import InputLineageReaderXML


class TestInputReaderXML:

    @pytest.fixture
    def xml_base(self, fill: str):
        return f"""
        <?xml version="1.0" encoding="Windows-1252"?>
        <!DOCTYPE POWERMART SYSTEM "powrmart.dtd">
            <POWERMART CREATION_DATE="08/22/2020 00:43:07" REPOSITORY_VERSION="185.94">
                <REPOSITORY NAME="INF_REP_DEV" VERSION="185" CODEPAGE="MS1252" DATABASETYPE="Oracle">
                    <FOLDER NAME="CollibraSample" GROUP="" OWNER="IDWBI" SHARED="NOTSHARED" DESCRIPTION="" PERMISSIONS="rwx---r--" UUID="dd8f69f3-5d32-40e8-bade-d623d45b5114">
                        {fill}
                    </FOLDER>
                </REPOSITORY>
            </POWERMART>
        """

    def test_xml_reader_basic(self, xml_base):
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
        """
