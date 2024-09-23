from flask import Flask, render_template, request
import dash
from dash import dcc, html
import plotly.graph_objects as go
import pandas as pd

# Step 1: Initialize Flask app
server = Flask(__name__)

# Step 2: Create a Dash app and pass Flask's `server` to Dash
dash_app = dash.Dash(__name__, server=server, url_base_pathname='/dash/')

# Step 3: Create the layout for Dash app
dash_app.layout = html.Div([
    html.H1("Sankey Diagram"),
    dcc.Graph(id="sankey_graph"),
    dcc.Slider(id="slider", min=0, max=1, value=0.5, step=0.1),
])

# Step 4: Create Dash callback to update the graph
@dash_app.callback(
    dash.dependencies.Output("sankey_graph", "figure"),
    [dash.dependencies.Input("slider", "value")]
)
def update_graph(opacity_value):
    # This would be the graph logic. For demonstration, we'll use a simple Sankey example.
    data = pd.DataFrame({
        'source': ['A', 'A', 'B'],
        'target': ['B', 'C', 'D'],
        'value': [10, 20, 30]
    })

    sources = data['source']
    targets = data['target']
    values = data['value']

    labels = list(set(sources) | set(targets))
    label_to_index = {label: i for i, label in enumerate(labels)}
    
    fig = go.Figure(go.Sankey(
        node=dict(label=labels),
        link=dict(source=[label_to_index[x] for x in sources],
                  target=[label_to_index[x] for x in targets],
                  value=values),
    ))

    fig.update_layout(
        title_text="Sankey Diagram with Opacity {}".format(opacity_value),
        font_size=10
    )
    
    return fig

# Step 5: Define routes for the Flask app
@server.route('/')
def index():
    return render_template('index.html')

@server.route('/input', methods=['POST'])
def input_data():
    # Retrieve user input from a form and process it (e.g., run a query)
    user_input = request.form.get('user_input')
    # Process the input or query your database
    # Then you could redirect the user to the Dash app or a different route
    return f"User Input: {user_input}"

# Step 6: Run the server
if __name__ == '__main__':
    server.run(debug=True)
