import pytest


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
