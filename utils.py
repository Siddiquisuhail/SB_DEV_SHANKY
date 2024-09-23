import pandas as pd 
import numpy as np 
from connections import Connections
from sqlalchemy import text 
from datetime import datetime, timedelta


conn = Connections() 


def fetch_data(start_date, end_date):

    query = f"""
    (select user_id, 
        (case when arcade_type = 2 then 'Poker' 
            when arcade_type = 3 then 'Rummy'
            when arcade_type = 9 then 'Ludo'
        end) play_type,
        min(date_added) ftp 
    from bb_datalake_validated.bb_poker_transactions  
    where user_id in 
        (select user_id 
        from bb_datalake_validated.bb_users 
        where registered_date between TIMESTAMP '{start_date}' 
                                and TIMESTAMP '{end_date}' 
        group by 1)
    and arcade_type in (2, 3, 9)
    group by 1, 2) 

    union all

    (select user_id, 'DFS' play_type, min(date_added) ftp 
    from bb_datalake_processed.game_transactions 
    where admin_status=2 
    and league_status=1 
    and user_id in 
        (select user_id 
        from bb_datalake_validated.bb_users 
        where registered_date between TIMESTAMP '{start_date}' 
                                and TIMESTAMP '{end_date}' 
        group by 1)
    group by 1)

    union all

    (select user_id , 'Stats_Pro' play_type, min(ftp) ftp 
    from (
        (select userid user_id, min(a.createdat) ftp 
        from bb_datalake_validated.bb_pending_bids_historical a 
        join bb_deltalake_validated.bb_market b on a.marketId = b.marketId  
        where b.marketstatus=3 
        and userid in 
            (select user_id 
            from bb_datalake_validated.bb_users 
            where registered_date between TIMESTAMP '{start_date}' 
                                    and TIMESTAMP '{end_date}' 
            group by 1) 
        group by 1)
    union all
        (select user_id, min(transaction_date) ftp 
        from bb_deltalake_validated.bb_stats_pro 
        where  user_id in 
            (select user_id 
            from bb_datalake_validated.bb_users 
            where registered_date between TIMESTAMP '{start_date}' 
                                    and TIMESTAMP '{end_date}' 
            group by 1) 
        group by 1)
    )
    group by 1)

    union all

    (select user_id, 'Predictor_Leagues' play_type, min(date_added) ftp 
    from bb_deltalake_validated.bb_tsp_user_leagues  
    where user_id in 
        (select user_id 
        from bb_datalake_validated.bb_users 
        where registered_date between TIMESTAMP '{start_date}' 
                                and TIMESTAMP '{end_date}' 
        group by 1) 
    group by 1)

    union all

    (select user_id, 'Player_Stocks' play_type, min(transaction_date) ftp 
    from bb_deltalake_validated.bb_player_stocks  
    where user_id in 
        (select user_id 
        from bb_datalake_validated.bb_users 
        where registered_date between TIMESTAMP '{start_date}' 
                                and TIMESTAMP '{end_date}' 
        group by 1) 
    group by 1)

    union all 

    (select user_id, 'deposit' play_type, min(date_added) ftp 
    from bb_datalake_validated.bb_txn 
    where status = 1 
    and user_id in 
        (select user_id 
        from bb_datalake_validated.bb_users 
        where registered_date between TIMESTAMP '{start_date}' 
                                and TIMESTAMP '{end_date}' 
        group by 1) 
    group by 1)

    union all

    (select user_id, 'registered_date' play_type, registered_date ftp 
    from bb_datalake_validated.bb_users 
    where registered_date between TIMESTAMP '{start_date}' 
                            and TIMESTAMP '{end_date}' )
    """



    with conn.athena_connect() as connection:

        rd = pd.read_sql_query(query, connection)
    
    return rd


def data_processing(df):
    df['ftp'] = pd.to_datetime(df['ftp'])

    df['rank'] = df.groupby(['user_id'])['ftp'].rank(method ='dense', ascending=True)

    df = df.sort_values(by = ['user_id', 'rank']).reset_index()

    df['diff'] = df.groupby('user_id')['ftp'].diff()
    df['diff'] = df['diff'].dt.total_seconds() / (60*60)

    df = df.rename(columns={'play_type' :'league_category'})


    df['prev_play_type'] = df.groupby('user_id')['league_category'].shift(1)

    df = df.dropna(subset='diff')


    df = df.rename(columns={'league_category' : 'target', 'prev_play_type' : 'source'})

    df = df.groupby(['source', 'target']).agg({'user_id':'nunique', 'diff' : 'median'}).reset_index()

    df = df.rename(columns={'user_id' : 'uniques'})

    df = df.rename(columns={'diff' : 'avg_time'})
    
    return df
    