import os
import sqlalchemy as sa
from connection_utils import connectDB, retrieveData, retrieveData_stationcode
import plotly.express as px
from dash import Dash, dcc, html, Input, Output,callback

# DB connection
USER=os.getenv("POSTGRES_USER")
PASSWORD=os.getenv("POSTGRES_PASSWORD")
HOST="db_app"
PORT=5432
DATABASE=os.getenv("POSTGRES_DB")

engine = connectDB(username=USER, host=HOST, password=PASSWORD, database=DATABASE)

# retrieve tables objects
metadata = sa.schema.MetaData()
metadata.reflect(engine)
historic_table = metadata.tables["historic"]
station_table = metadata.tables["station"]

# get data (need refresh ?)
station = retrieveData(engine, station_table).sort_values("name").set_index("stationcode")
#hist = retrieveData(engine, table="historic")
name_to_code = {v:k for k,v in station["name"].to_dict().items()}

app = Dash(__name__)

server = app.server

app.layout = html.Div([
    dcc.Dropdown(station["name"].tolist(), '11 Novembre 1918 - 8 Mai 1945', id='station_name'),
    dcc.Graph(id='graph')
        ]
    )

@callback(
    Output('graph', 'figure'),
    Input('station_name', 'value'),
)
def update_figure(selected_station_name):
    stationcode = name_to_code[selected_station_name]
    #data = hist.loc[hist.stationcode == stationcode]
    data = retrieveData_stationcode(engine, historic_table, stationcode)
    data = data.groupby("duedate").min().reset_index() # duplicate management
    if data.shape[0] > 0:
        fig = px.line(data,x="duedate", y=["ebike","mechanical", "numbikesavailable"],markers="o")
        return fig
    else:
        return None
        
if __name__ == "__main__":
    app.run(debug=True)
