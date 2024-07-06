from typing import List, Tuple
from datetime import datetime
import pandas as pd

def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    df = pd.read_parquet(file_path, columns=['id', 'username', 'date'])
    df['date_only'] = pd.to_datetime(df['date']).dt.date
    
    top_10_dates = df.groupby('date_only').size().nlargest(10).reset_index(name='tweetCount_date')
    top_user_by_date = df[df['date_only'] \
        .isin(top_10_dates['date_only'])] \
        .groupby(['date_only', 'username']) \
        .size().nlargest(10).reset_index(name='tweetCount_user')

    result = top_10_dates.merge(top_user_by_date, on='date_only').sort_values('tweetCount_date', ascending=False)

    return [(row.date_only, row.username) for row in result.itertuples()]