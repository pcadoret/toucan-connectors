import pytest

from toucan_connectors.workday_xml.workday_xml_connector import WorkdayXMLConnector, WorkdayXMLDataSource

@pytest.fixture(scope='function')
def connector():
    return WorkdayXMLConnector(
        name='myWorkdayConnector',
        type='Workday',
        tenant='umanis',
        username='<username>',
        password='<password>',
    )


@pytest.fixture(scope='function')
def data_source():
    return WorkdayXMLDataSource(
        name='myWorkdayDataSource',
        domain='Workers',
        namespaces= {"wd": "urn:com.workday.report/All_Worker_Time_Off"},
	operations= [{"xpath": ".//wd:Date_de_l_absence", "attribute": None, "column": "Date_Absence"}],
    	report_url= 'https://wd3-impl-services1.workday.com/ccx/service/systemreport2/umanis/All_Worker_Time_Off?Organisations!WID={0}&Inclure_les_responsables=0&Inclure_les_organisations_subordonn%C3%A9es=1',
    )


def test_get_df(connector, data_source):
    df = connector.get_df(data_source)
    print(df['Org_Parent_WID'])
    assert df.shape == (2, 2)

