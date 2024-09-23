import plotly.graph_objects as go
import plotly.io as pio 
import chart_studio
import chart_studio.plotly as py 
from dateutil.relativedelta import relativedelta
from plotly.subplots import make_subplots
from dash import Dash, dcc, html, Input, Output
from fastapi import FastAPI
from starlette.middleware.wsgi import WSGIMiddleware
import uvicorn
from flask import Flask, render_template, request, redirect, url_for, flash, session
from dash import Dash, dcc, html, Input, Output
from datetime import datetime, timedelta
from utils import fetch_data, data_processing
import pandas as pd
import numpy as np 



server = Flask(__name__)

server.secret_key = 'your_secret_key' 




dash_app = Dash(__name__, server=server, url_base_pathname='/dash/')


# External stylesheets if needed
external_stylesheet = 'assets/custom_style.css'


dash_app.layout = html.Div(
    [
        html.H1(f"FTD Migration Graph"),
        dcc.Graph(id="graph"),
        html.Button("Update Graph", id="update-button"),
        # html.P("Adjust Opacity:"),
    ]
)

# Define Dash callback
@dash_app.callback(
    Output("graph", "figure"),
    Input("update-button", "n_clicks")  # Trigger on button click
)
def update_graph(n_clicks):
    if n_clicks is None:
        return go.Figure()  # Return an empty figure if no clicks
    
    # Retrieve processed data from session
    data = session.get('processed_data')

    if data is None:
        print("No data found in session")
        return go.Figure()  # Return empty figure

    print(f"Data retrieved from session: {data}")  # Debugging print statement

    # Convert dictionary back to DataFrame
    data_df = pd.DataFrame(data)
    print(f"DataFrame created: {data_df.head()}")

    # Proceed with generating the graph
    sources = data_df['source'].to_list()
    targets = data_df['target'].to_list()
    value = data_df['uniques'].to_list()

    labels = list(set(sources + targets))
    label_to_index = {label: i for i, label in enumerate(labels)}

    source_indices = [label_to_index[label] for label in sources]
    target_indices = [label_to_index[label] for label in targets]

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
            value=value,
            hovertemplate='Source: %{source.label}<br>Target: %{target.label}<br>Users: %{value}<br>Median Time: %{customdata} hours',
            customdata=data_df['avg_time'].round(2).tolist(),
        )
    )])

    fig.update_layout(
        title_text="FTD Migration",
        title_x=0.5,
        font_size=14,
        title_font_size=24,
        width=1200,
        height=800,
        margin=dict(l=100, r=100, t=100, b=100),
        paper_bgcolor="white"
    )

    return fig

# Flask Routes
@server.route('/')
def index():
    current_year = datetime.now().year
    return render_template('index.html', current_year=current_year)

@server.route('/input', methods=['POST'])
def handle_input():
    current_year = datetime.now().year
    year = int(request.form.get('year'))
    month = request.form.get('month')

    month_number = datetime.strptime(month, "%B").month
    first_day = datetime(year, month_number, 1)
    last_day = (first_day + relativedelta(months=1)) - timedelta(days=1)

    first_day_str = first_day.strftime('%Y-%m-%d %H:%M:%S')
    last_day_str = last_day.strftime('%Y-%m-%d %H:%M:%S')

    # Fetch and process data
    data = fetch_data(first_day_str, last_day_str)
    processed_data = data_processing(data)

    print(f"Processed data before saving to session: {processed_data.head()}")

    # Save processed_data as a dictionary in the session
    session['processed_data'] = processed_data.to_dict()

    return redirect(url_for('dash_app_index'))

@server.route('/dash/')
def dash_app_index():
    return dash_app.index()

# Run the server
if __name__ == '__main__':
    server.run(debug=True)