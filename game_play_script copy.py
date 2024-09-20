import pandas as pd 
import numpy as np 
import matplotlib.pyplot as plt 
import seaborn as sns 
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
from overall_script import data_extraction

data_extraction()


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

rd = pd.read_csv(f"data/{start_of_previous_month.date()}_to_{end_of_previous_month.date()}_data.csv")


rd['ftp'] = pd.to_datetime(rd['ftp'])

rd['rank'] = rd.groupby(['user_id'])['ftp'].rank(method ='dense', ascending=True)

rd = rd.sort_values(by = ['user_id', 'rank']).reset_index()

rd['diff'] = rd.groupby('user_id')['ftp'].diff()
rd['diff'] = rd['diff'].dt.total_seconds() / (60*60)

rd = rd.rename(columns={'play_type' :'league_category'})


rd['prev_play_type'] = rd.groupby('user_id')['league_category'].shift(1)

rd = rd.dropna(subset='diff')

# organic_userids =


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
    title_text="August Overall FTP User Game Migrations",
    title_x=0.5,
    title_y=0.95,
    font_size=14,
    title_font_size=24,
    width=1200,
    height=800,
    margin=dict(l=100, r=100, t=100, b=100),
    paper_bgcolor="white"
)


pio.write_html(fig, file='plot.html', auto_open=True)

chart_studio.tools.set_credentials_file(username = 'Suhail.shamim', api_key = 'LSRgHhbz4KYJRoqQv8rE' )

url = py.plot(fig, filename='aug_plot.html', auto_open=True)

