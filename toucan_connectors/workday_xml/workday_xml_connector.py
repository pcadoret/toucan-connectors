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
        example='https://wd3-impl-services1.workday.com/ccx/service/systemreport2/umanis/All_Worker_Time_Off?Inclure_les_responsables=0&Inclure_les_organisations_subordonn%C3%A9es=1',
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
        connector = WorkdayConnector(
            name='myWorkdayConnector',
            type='Workday',
            tenant=self.tenant,
            username=self.username,
            password=self.password,
        )

        data_source_organizations = WorkdayDataSource(
            name='myWorkdayDataSource',
            domain='Referentiel',
            service='Human_Resources',
            service_WSDL_URL='https://wd3-impl-services1.workday.com/ccx/service/umanis/Human_Resources/v33.2',
            operation='Get_Organizations',
            request_parameters={
                'Request_References': None,
                'Request_Criteria': None,
                'Response_Filter': {},
                'Response_Group': None
            },
            filter='[.Organization[] | {Org_WID: .Organization_Reference.ID[0]._value_1, Org_Parent_WID: .Organization_Data.Hierarchy_Data.Superior_Organization_Reference.ID[0]._value_1, Org_SubType: .Organization_Data.Organization_Subtype_Reference.ID[1]._value_1}]',
        )

        df_organizations = connector.get_df(data_source_organizations)
        
        url_report_Absences =  data_source.report_url + '&Organisations!WID={0}' \
    .format('!'.join(df_organizations.loc[df_organizations.Org_Parent_WID.isnull() & (df_organizations.Org_SubType == "Matrix"),"Org_WID"]))
        print(url_report_Absences)
        
        return df_organizations
