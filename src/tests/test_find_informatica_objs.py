import pytest
import xml.etree.ElementTree as ET
from unittest.mock import patch, MagicMock
from src.InputLineageReaderXML import InputLineageReaderXML
from main import find_informatica_objs


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
    def informatica_objs_xml_fill(self):
        return """
        <WORKFLOW ISENABLED ="YES" ISRUNNABLESERVICE ="NO" ISSERVICE ="NO" ISVALID ="YES" NAME ="collibra_sample" REUSABLE_SCHEDULER ="NO" SCHEDULERNAME ="Scheduler" SERVERNAME ="INF_INT_DEV" SERVER_DOMAINNAME ="Domain" SUSPEND_ON_ERROR ="NO" TASKS_MUST_RUN_ON_SERVER ="NO" VERSIONNUMBER ="1">
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_orderentry_internetsales" NAME ="oracle" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_refine_customersalesreport" NAME ="sql" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <TASKINSTANCE NAME ="oracle" TASKNAME ="oracle" TASKTYPE ="Session"/>
            <TASKINSTANCE NAME ="sql" TASKNAME ="sql" TASKTYPE ="Session"/>
        </WORKFLOW>
        <MAPPING ISVALID ="YES" NAME ="m_refine_customersalesreport" OBJECTVERSION ="1" VERSIONNUMBER ="1">
            <TRANSFORMATION NAME ="SQ_FactInternetSales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="DueDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
            </TRANSFORMATION>
            <TRANSFORMATION NAME ="SQ_FactInternetSales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="DueDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
            </TRANSFORMATION>
        </MAPPING>
        <MAPPING ISVALID ="YES" NAME ="m_orderentry_internetsales" OBJECTVERSION ="1" VERSIONNUMBER ="1">
            <TRANSFORMATION NAME ="SQ_FactInternetSales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Source Qualifier" VERSIONNUMBER ="1">
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ProductKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="OrderDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="DueDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="integer" DEFAULTVALUE ="" NAME ="ShipDateKey" PORTTYPE ="INPUT/OUTPUT" PRECISION ="10" SCALE ="0"/>
            </TRANSFORMATION>
            <TRANSFORMATION NAME ="m_orderentry_internetsales" OBJECTVERSION ="1" REUSABLE ="NO" TYPE ="Expression" VERSIONNUMBER ="1">
                <TRANSFORMFIELD DATATYPE ="nstring" DEFAULTVALUE ="" NAME ="FirstName" PORTTYPE ="INPUT" PRECISION ="50" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="nstring" DEFAULTVALUE ="ERROR(&apos;transformation error&apos;)" EXPRESSION ="CONCAT(FirstName, CONCAT(MiddleName,LastName))" EXPRESSIONTYPE ="GENERAL" NAME ="NewFullName" PORTTYPE ="OUTPUT" PRECISION ="10" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="nstring" DEFAULTVALUE ="" NAME ="MiddleName" PORTTYPE ="INPUT" PRECISION ="50" SCALE ="0"/>
                <TRANSFORMFIELD DATATYPE ="nstring" DEFAULTVALUE ="" NAME ="LastName" PORTTYPE ="INPUT" PRECISION ="50" SCALE ="0"/>
            </TRANSFORMATION>
        </MAPPING>
        """

    @pytest.fixture
    def session_referencing_wrong_mapping_xml_fill(self):
        return """
        <WORKFLOW ISENABLED ="YES" ISRUNNABLESERVICE ="NO" ISSERVICE ="NO" ISVALID ="YES" NAME ="collibra_sample" REUSABLE_SCHEDULER ="NO" SCHEDULERNAME ="Scheduler" SERVERNAME ="INF_INT_DEV" SERVER_DOMAINNAME ="Domain" SUSPEND_ON_ERROR ="NO" TASKS_MUST_RUN_ON_SERVER ="NO" VERSIONNUMBER ="1">
            <SESSION ISVALID ="YES" MAPPINGNAME ="m_orderentry_internetsales" NAME ="oracle" REUSABLE ="NO" SORTORDER ="Binary" VERSIONNUMBER ="1">
            </SESSION>
            <TASKINSTANCE NAME ="oracle" TASKNAME ="oracle" TASKTYPE ="Session"/>
        </WORKFLOW>
        <MAPPING ISVALID ="YES" NAME ="m_refine_customersalesreport" OBJECTVERSION ="1" VERSIONNUMBER ="1">
            <TRANSFORMFIELD DATATYPE ="nstring" DEFAULTVALUE ="" NAME ="MiddleName" PORTTYPE ="INPUT" PRECISION ="50" SCALE ="0"/>
            <TRANSFORMFIELD DATATYPE ="nstring" DEFAULTVALUE ="" NAME ="LastName" PORTTYPE ="INPUT" PRECISION ="50" SCALE ="0"/>
        </MAPPING>
        """

    def test__check_duplicates(self, xml_base, informatica_objs_xml_fill):
        mock_xml = ET.fromstring(xml_base(informatica_objs_xml_fill))
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_root = MagicMock()
        mock_root.getroot.return_value = mock_xml
        with patch('src.InputLineageReaderXML.etree.parse', return_value=mock_root):
            xml = InputLineageReaderXML(mock_file)
            res = find_informatica_objs(xml)
            assert res == {
                'INF_REP_DEV':
                    {
                        'CollibraSample':
                            {
                                'collibra_sample':
                                    {
                                        'oracle':
                                            {
                                                'm_orderentry_internetsales':
                                                    {
                                                        'SQ_FactInternetSales':
                                                            {
                                                                'ProductKey': {'id': 1},
                                                                'OrderDateKey': {'id': 2},
                                                                'DueDateKey': {'id': 3},
                                                                'ShipDateKey': {'id': 4}
                                                            },
                                                        'm_orderentry_internetsales':
                                                            {
                                                                'FirstName': {'id': 5},
                                                                'NewFullName': {'id': 6},
                                                                'MiddleName': {'id': 7},
                                                                'LastName': {'id': 8}
                                                            }
                                                    }
                                            },
                                        'sql':
                                            {
                                                'm_refine_customersalesreport':
                                                    {
                                                        'SQ_FactInternetSales':
                                                            {
                                                                'ProductKey': {'id': 9},
                                                                'OrderDateKey': {'id': 10},
                                                                'DueDateKey': {'id': 11},
                                                                'ShipDateKey': {'id': 12}
                                                            }
                                                    }
                                            }
                                    }
                            }
                    }
            }
