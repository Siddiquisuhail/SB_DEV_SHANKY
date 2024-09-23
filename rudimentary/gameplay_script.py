import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 
from connections import Connections
import pprint as pp
import datetime as dt
import plotly.graph_objects as go
import plotly.io as pio 
import chart_studio
import chart_studio.plotly as py 
import datetime 
from datetime import time, date
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash import Dash, dcc, html, Input, Output
import plotly.graph_objects as go
import json, urllib
import pprint as pp
from sqlalchemy import text 
from fastapi import FastAPI
from starlette.middleware.wsgi import WSGIMiddleware
import uvicorn




conn = Connections() 











current_date = datetime.datetime.now()

# Calculate the first day of the current month
first_day_of_current_month = datetime.datetime(current_date.year, current_date.month, 1)

# Calculate the first day of the previous month
start_of_previous_month = first_day_of_current_month - relativedelta(months=1)

# Calculate the last day of the previous month
end_of_previous_month = first_day_of_current_month - datetime.timedelta(seconds=1)

# Format both dates
start_of_previous_month_formatted = start_of_previous_month.strftime('%Y-%m-%d %H:%M:%S')
end_of_previous_month_formatted = end_of_previous_month.strftime('%Y-%m-%d %H:%M:%S')

# print("Start of previous month:", start_of_previous_month_formatted)
# print("End of previous month:", end_of_previous_month_formatted)

# Calculate the start of the previous day
start_of_previous_day = (current_date - datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

# Calculate the end of the previous day
end_of_previous_day = (current_date - datetime.timedelta(days=1)).replace(hour=23, minute=59, second=59, microsecond=999999)

# Format both dates
start_of_previous_day_formatted = start_of_previous_day.strftime('%Y-%m-%d %H:%M:%S')
end_of_previous_day_formatted = end_of_previous_day.strftime('%Y-%m-%d %H:%M:%S')



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
     where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                             and TIMESTAMP '{end_of_previous_month_formatted}' 
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
     where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                             and TIMESTAMP '{end_of_previous_month_formatted}' 
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
         where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                                 and TIMESTAMP '{end_of_previous_month_formatted}' 
         group by 1) 
     group by 1)
union all
    (select user_id, min(transaction_date) ftp 
     from bb_deltalake_validated.bb_stats_pro 
     where  user_id in 
        (select user_id 
         from bb_datalake_validated.bb_users 
         where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                                 and TIMESTAMP '{end_of_previous_month_formatted}' 
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
     where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                             and TIMESTAMP '{end_of_previous_month_formatted}' 
     group by 1) 
 group by 1)

union all

(select user_id, 'Player_Stocks' play_type, min(transaction_date) ftp 
 from bb_deltalake_validated.bb_player_stocks  
 where user_id in 
    (select user_id 
     from bb_datalake_validated.bb_users 
     where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                             and TIMESTAMP '{end_of_previous_month_formatted}' 
     group by 1) 
 group by 1)

union all 

(select user_id, 'deposit' play_type, min(date_added) ftp 
 from bb_datalake_validated.bb_txn 
 where status = 1 
   and user_id in 
    (select user_id 
     from bb_datalake_validated.bb_users 
     where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                             and TIMESTAMP '{end_of_previous_month_formatted}' 
     group by 1) 
 group by 1)

union all

(select user_id, 'registered_date' play_type, registered_date ftp 
 from bb_datalake_validated.bb_users 
 where registered_date between TIMESTAMP '{start_of_previous_month_formatted}' 
                         and TIMESTAMP '{end_of_previous_month_formatted}' )
"""



with conn.athena_connect() as connection:

    rd = pd.read_sql_query(query, connection)
    # rd.to_csv('test.csv', index=False)

    
    

# rd = df.copy()


rd['ftp'] = pd.to_datetime(rd['ftp'])

rd['rank'] = rd.groupby(['user_id'])['ftp'].rank(method ='dense', ascending=True)

rd = rd.sort_values(by = ['user_id', 'rank']).reset_index()

rd['diff'] = rd.groupby('user_id')['ftp'].diff()
rd['diff'] = rd['diff'].dt.total_seconds() / (60*60)

rd = rd.rename(columns={'play_type' :'league_category'})


rd['prev_play_type'] = rd.groupby('user_id')['league_category'].shift(1)

rd = rd.dropna(subset='diff')



data = rd.copy()

data = data.rename(columns={'league_category' : 'target', 'prev_play_type' : 'source'})

data = data.groupby(['source', 'target']).agg({'user_id':'nunique', 'diff' : 'median'}).reset_index()

data = data.rename(columns={'user_id' : 'uniques'})

data = data.rename(columns={'diff' : 'avg_time'})

sources = data['source'].to_list()

targets = data['target'].to_list()

value = data['uniques'].to_list()

labels = list(set(sources + targets))



label_to_index = {label: i for i, label in enumerate(labels)}
source_indices = [label_to_index[label] for label in sources]
target_indices = [label_to_index[label] for label in targets]

# Create the Sankey diagram
external_stylesheet = 'assets/custom_style.css'
dash_app = Dash(__name__, external_stylesheet)

dash_app.layout = html.Div(
    [
        html.H1(f"FTD Migration Graph for {start_of_previous_month.month}th Month. "),
        dcc.Graph(id="graph"),
        html.P("Opacity"),
        dcc.Slider(id="slider", min=0, max=1, value=0.5, step=0.1),
    ]
)


@dash_app.callback(
    Output("graph", "figure"),
    Input("slider", "value")  # Added input for the slider, though not used directly in Sankey
)
def display_sankey(opacity_value):  # Slider value added as an argument
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            label=labels,
        ),
        link=dict(
            source=source_indices,
            target=target_indices,
            value=data['uniques'].tolist(),
            hovertemplate='Source: %{source.label}<br>Target: %{target.label}<br>Users: %{value}<br>Median Time: %{customdata} hours',
            customdata=data['avg_time'].round(2).tolist(),
        )
    )])

    fig.update_layout(
        title_text= f"{start_of_previous_month.month}",
        title_x=0.5,
        title_y=0.95,
        font_size=14,
        title_font_size=24,
        width=1200,
        height=800,
        margin=dict(l=100, r=100, t=100, b=100),
        paper_bgcolor="white"
    )

    return fig


if __name__ == "__main__":
    dash_app.run_server(debug=True)


# Create the FastAPI app
# app = FastAPI()

# # Mount the Dash app onto FastAPI
# app.mount("/", WSGIMiddleware(dash_app.server))

# if __name__ == "__main__":
#     # Run the app using uvicorn
#     uvicorn.run(app, host="127.0.0.1", port=8050)