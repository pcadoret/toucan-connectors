import pandas as pd
from pydantic import Field
import requests
import xml.etree.ElementTree as et

from toucan_connectors.toucan_connector import ToucanConnector, ToucanDataSource
from toucan_connectors.workday.workday_connector import WorkdayConnector, WorkdayDataSource


class WorkdayXMLDataSource(ToucanDataSource):
    namespaces: dict = Field(
        None,
        title='',
        description='',
        example='{"wd": "urn:com.workday.report/All_Worker_Time_Off"}',
    )
    operations: list = Field(
        None,
        title='',
        description='',
        example='[{"xpath": ".//wd:Date_de_l_absence", "attribute": None, "column": "Date_Absence"}]',
    )
    report_url: str = Field(
        None,
        title='',
        description='',
        example='https://wd3-impl-services1.workday.com/ccx/service/systemreport2/umanis/All_Worker_Time_Off?Organisations!WID={0}&Inclure_les_responsables=0&Inclure_les_organisations_subordonn%C3%A9es=1',
    )

class WorkdayXMLConnector(ToucanConnector):
    data_source_model: WorkdayXMLDataSource

    tenant: str = Field(
        None,
        title='Name of the Workday tenant',
        description='Name of the Workday tenant',
        example='my_tenant',
    )
    username: str = Field(
        None,
        title='Username for authentification',
        description='Username for Credential authentification. Leave empty for Anonymous authentification.',
        example='my_username',
    )
    password: str = Field(
        None,
        title='Password for authentification',
        description='Password for Credential authentification',
        example='$ecreTP4s$w0rd!',
    )

    def _retrieve_data(self, data_source: WorkdayXMLDataSource) -> pd.DataFrame:
        pass
        

