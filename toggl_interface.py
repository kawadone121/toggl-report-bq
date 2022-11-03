# -*- coding: utf-8 -*-

import requests, time
from requests.auth import HTTPBasicAuth
import pandas as pd


class Toggl:
    """
    Toggl APIよりデータを取得
    API docs: https://github.com/toggl/toggl_api_docs
    """
    def __init__(self, api_token, mail_address):
        self.api_token = api_token
        self.mail_address = mail_address
        self.workspace_id = self.get_workspace_id()

    def get_workspace_id(self):
        """
        ワークスペースIDを取得
        """
        r = requests.get(
            'https://api.track.toggl.com/api/v8/workspaces',
            auth=(self.api_token, 'api_token')
        )
        workspace_id = r.json()[0]['id']
        return workspace_id

    def get_detailed_report(self, target_dates):
        """
        指定した日付の詳細レポートを取得
        
        Note:
            1回のリクエストで取得できるイベント数の最大値は50
        """
        df = pd.DataFrame()
        params = {
            'user_agent': self.mail_address,
            'workspace_id': self.workspace_id
        }
        for target_date in target_dates:
            params['since'], params['until'] = target_date, target_date
            r = requests.get(
                'https://api.track.toggl.com/reports/api/v2/details',
                auth=HTTPBasicAuth(self.api_token, 'api_token'),
                params=params
            )
            data = r.json()['data']
            df_temp = pd.DataFrame(data)
            df = pd.concat([df, df_temp], axis=0)
            time.sleep(1)
        print('Got detailed report from {} to {}.'.format(target_dates[0], target_dates[-1]))
        return df
