# -*- coding: utf-8 -*-

from os import getenv
from datetime import datetime, timedelta
from traceback import format_exception_only
from dotenv import load_dotenv
import pandas as pd
from google.cloud.bigquery import SchemaField

from toggl_interface import Toggl
from bq_interface import BigQuery
from slack_interface import Slack

load_dotenv()
TOGGL_API_TOKEN = getenv('TOGGL_API_TOKEN')
TOGGL_MAIL_ADDRESS = getenv('TOGGL_MAIL_ADDRESS')
BQ_TABLE_ID = getenv('BQ_TABLE_ID')
SLACK_OAUTH_TOKEN = getenv('SLACK_OAUTH_TOKEN')

BQ_TABLE_SCHEMA = [
    SchemaField('id',          'INTEGER',   'REQUIRED'),
    SchemaField('project',     'STRING',    'NULLABLE'),    
    SchemaField('description', 'STRING',    'NULLABLE'),
    SchemaField('start',       'TIMESTAMP', 'REQUIRED'),
    SchemaField('end',         'TIMESTAMP', 'REQUIRED'),
    SchemaField('updated',     'TIMESTAMP', 'REQUIRED'),
    SchemaField('second',      'INTEGER',   'REQUIRED'),
    SchemaField('minute',      'FLOAT',     'REQUIRED'),
    SchemaField('hour',        'FLOAT',     'REQUIRED'),
    SchemaField('tag1',        'STRING',    'NULLABLE'),
    SchemaField('tag2',        'STRING',    'NULLABLE'),
    SchemaField('tag3',        'STRING',    'NULLABLE'),
]
TIME_PARTITIONING_FIELD = 'start'
CLUSTERING_FIELDS = ['project', 'tag1', 'tag2', 'tag3']
ROLLBACK_DAYS = 7
TARGET_SLACK_CHANNEL = 'error'

def main():
    """
    Toggl API経由で詳細レポートを取得しBigQueryに最新データを格納
    """
    try:
        toggl = Toggl(TOGGL_API_TOKEN, TOGGL_MAIL_ADDRESS)
        bq = BigQuery(BQ_TABLE_ID, BQ_TABLE_SCHEMA, TIME_PARTITIONING_FIELD, CLUSTERING_FIELDS)
        slack = Slack(SLACK_OAUTH_TOKEN)

        target_dates = [(datetime.now() - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(ROLLBACK_DAYS)][::-1]
        start_date = target_dates[0]
        end_date = target_dates[-1]

        df = toggl.get_detailed_report(target_dates)
        df_formatted = get_formatted_dataframe(df)
        bq.create_table_if_not_exits()
        bq.update(
            df_formatted, 
            start='{} 00:00:00'.format(start_date), 
            end='{} 23:59:59'.format(end_date), 
            timestamp_col='start'
        )
    except Exception as e:
        error_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        error_summary = format_exception_only(type(e), e)[0].rstrip('\n')
        message = '{} {}'.format(error_timestamp, error_summary)
        slack.send_message(TARGET_SLACK_CHANNEL, message)

def get_formatted_dataframe(data):
    """
    Toggl API経由で取得した詳細レポートをDataframeに整形
    """
    use_cols = [
        'id', 
        'project',     # プロジェクト未設定の場合None
        'description', # 名称未設定の場合None
        'tags',        # ['tag1', 'tag2', 'tag3',,,]
        'start',       # (YYYY-MM-DDTHH:MM:SS)
        'end',         # (YYYY-MM-DDTHH:MM:SS)
        'updated',     # (YYYY-MM-DDTHH:MM:SS)
        'dur',         # millisecond
    ]
    df = pd.DataFrame(data)[use_cols]

    # ミリ秒を秒・分・時間に変換し列追加
    df = add_duration_columns(df, colname='dur')

    # タグを抽出し列に分割
    df = add_tag_columns(df, num_tags=3, colname='tags')

    # データ型を明示的に指定
    dtypes = {
        'id': 'int64', 
        'project': 'object', 
        'description': 'object', 
        'second': 'int64', 
        'minute': 'float64',
        'hour':'float64',
        'tag1': 'object', 
        'tag2': 'object', 
        'tag3': 'object',
    }
    datetime_cols = ['start', 'end', 'updated']
    df = convert_dtypes(df, dtypes, datetime_cols)
    return df

def add_duration_columns(df, colname='dur'):
    """
    ミリ秒の列をベースに秒・分・時間の列を追加
    """
    df['second'] = df[colname] / 1000
    df['minute'] = df['second'] / 60
    df['hour'] = df['minute'] / 60
    df.drop(columns=colname, inplace=True)
    return df

def add_tag_columns(df, num_tags, colname='tags'):
    """
    リスト型のタグリストから指定した数のタグを抽出し、個別に列を作成
    """
    for i in range(num_tags):
        df['tag{}'.format(i+1)] = df[colname].apply(lambda x: extract_tag(x, i))
    df.drop(columns='tags', inplace=True)
    return df

def extract_tag(list, i):
    """
    リスト型のタグリストからタグを抽出
    """
    try:
        tag = list[i]
        return tag
    except:
        return None

def convert_dtypes(df, dtypes, datetime_cols):
    """
    Dataframeのdtypeを明示的に指定し変換
    """
    df = df.astype(dtypes)
    for col in datetime_cols:
        df[col] = pd.to_datetime(df[col])
    return df

if __name__ == '__main__':
    main()
