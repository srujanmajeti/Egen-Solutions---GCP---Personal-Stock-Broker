import dash
import dash_auth
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import plotly.graph_objects as go
import pandas as pd
import re
import datetime
import csv

# accessing google bigquery table
import google.auth
from google.cloud import bigquery
from google.cloud import bigquery_storage_v1beta1


credentials, your_project_id = google.auth.default(
    scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Make clients.
bqclient = bigquery.Client(
    credentials=credentials,
    project=your_project_id,
)
bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(
    credentials=credentials
)


# Download query results.
read_full_historic_table = """
SELECT * FROM `global-cursor-278922.stock_data.historic_table_gcp`
"""
read_full_alert_table = """
SELECT * FROM `global-cursor-278922.stock_data.alert_table_gcp`
"""



historic_table_gcp = (
    bqclient.query(read_full_historic_table)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)

alert_table_gcp = (
    bqclient.query(read_full_alert_table)
    .result()
    .to_dataframe(bqstorage_client=bqstorageclient)
)

sym_list = list(historic_table_gcp['Symbol'].unique())

my_options = []
for symbol in sym_list:
    temp_dict = {}
    temp_dict['label'] = symbol
    temp_dict['value'] = symbol
    my_options.append(temp_dict)



# setting style for the app
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config.suppress_callback_exceptions = True

# # creating a user symbol list dataframe
user_symbol_list_df = None


# for dash app
def get_color(a):
    if a % 2 == 0:
        return "#A0DCF6"
    else:
        return "#E9F5FB"


def generate_table(dataframe, max_rows=20):
    return html.Table(
        # Header
        [html.Tr([
            html.Th(col) for col in dataframe.columns
        ])
        ] +

        # Body
        [html.Tr([
            html.Td(
                dataframe.iloc[i][col],
                style={'padding-top': '2px', 'padding-bottom': '2px', 'text-align': 'right',
                       'backgroundColor': get_color(i)}
            ) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )


def generate_alert_table(dataframe, max_rows=20):
    return html.Table(
        # Header
        [html.Tr([
            html.Th(col) for col in dataframe.columns
        ])
        ] +

        # Body
        [html.Tr([
            html.Td(
                dataframe.iloc[i][col],
                style={'padding-top': '2px', 'padding-bottom': '2px', 'text-align': 'right', 'color': 'red',
                       'font-weight': 'bold'}
            ) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )


# Layout
app.layout = html.Div(
    [
        #             Style={‘background-image’: ‘url(C:/Users/sruja/OneDrive/insights program/insight_project1/new_dash/PB.jpg)’},
        html.Img(
            id='logo',
            src=app.get_asset_url("C:/Users/sruja/OneDrive/insights program/insight_project1/new_dash/assets/PB.jpg"),
            style={'width': '100px', 'height': '100px', 'display': 'inline-block'}
        ),
        html.H2(
            'Personal Broker',
            style={'color': 'blue', 'font-weight': 'bold', 'display': 'inline-block', 'position': 'absolute',
                   'margin': '40px 0'}
        ),
        html.H6(
            'Providing (near) Real-Time Analytics on your personal Portfolio',
            style={'color': 'blue', 'font-weight': 'bold', 'padding-top': '2px'}
        ),
        html.Hr(),

        html.H5(
            'Candlesticks',
            style={'color': 'blue', 'font-weight': 'bold', 'padding-top': '2px'}
        ),

        html.Div([
            dcc.Dropdown(id='demo-dropdown',
                         options=my_options,
                         value='A'
                         ),
            dcc.Graph(id='dd-output-container'),

            html.Hr(),
            html.Div([
                html.H5(
                    'Stock Alerts',
                    style={'color': 'blue', 'font-weight': 'bold', 'padding-top': '2px'}
                ),

                html.Div(
                    [
                        html.H6('List your Portfolio:'),
                        dcc.Input(
                            id='stock-ticker', value='AAL', type='text',
                            style={'width': '600px', 'backgroundColor': '#E8E8E8'}
                        ),  # human text input of stock ticker
                        html.Div(id='stock-records', style={'display': 'none'}),
                        html.H5('Valid entries', style={'color': 'blue', 'font-weight': 'bold'}),
                        html.Div(id='valid-ticker'),  # Output valid tickers
                        html.Div(id='stock-data'),
                        html.Hr(),
                    ],
                    style={'width': '50%', 'height': '100%', 'float': 'left'}
                ),
                html.Div(
                    [
                        html.H6('Alerts trigger when the Percentage Change is above:'),
                        dcc.Dropdown(
                            id='perc-dropdown',
                            options=[{'label': '2%', 'value': '2'}],
                            value='2',
                            style={'backgroundColor': '#E8E8E8'}
                        ),
                        html.H5('ALERT for Volatile Stocks', style={'color': 'red', 'font-weight': 'bold'}),
                        html.Div('The following stocks in your portfolio are changing rapidly:'),
                        html.Div(id='stock-alert-data')
                    ],
                    style={'width': '50%', 'height': '100%', 'float': 'left'}
                ),
            ], className='column'),

        ]),

        dcc.Interval(
            id='interval-component',
            interval=1 * 100000000,  # in milliseconds
            n_intervals=0
        )
    ]
)


# Store selected stock data dataframe in intermediate step.
@app.callback(Output('stock-records', 'children'),
              [Input('stock-ticker', 'value'), Input('interval-component', 'n_intervals')])
def query_stock_df(selected_ticker, n):
    selected_ticker1 = pd.Series(re.split('\W', selected_ticker))
    selected_ticker_upper = [x.upper() for x in selected_ticker1]
    selected_ticker_upper = list(dict.fromkeys(selected_ticker_upper))
    df = pd.DataFrame(columns=['Stock', 'Datetime', 'Current_Price', 'Daily_Average_Price', 'Percentage_Change'])
    for single_ticker in selected_ticker_upper:
        if single_ticker:
            query_string = """ SELECT * FROM `global-cursor-278922.stock_data.alert_table_gcp` where stock = '{}' """.format(single_ticker)
            df = df.append( bqclient.query(query_string)
                .result()
                .to_dataframe(bqstorage_client=bqstorageclient), ignore_index=True)
    df = df[['Stock', 'Datetime', 'Current_Price', 'Daily_Average_Price', 'Percentage_Change']]
    return df.to_json()


# Print the valid tickers.
@app.callback(Output('valid-ticker', 'children'),
              [Input('stock-records', 'children')]
              )
def print_valid_ticker(df_json):
    valid_ticker = pd.read_json(df_json)['Stock']
    return ', '.join(valid_ticker)


# Print the tables of selected stocks
@app.callback(Output('stock-data', 'children'),
              [Input('stock-records', 'children')]
              )
def plot_stock_df(df_json):
    stock_df = pd.read_json(df_json)
    stock_df['Percentage_Change'] = pd.Series(["{0:.2f}%".format(val) for val in stock_df['Percentage_Change']],
                                              index=stock_df.index)
    print(datetime.datetime.now())
    return generate_table(stock_df)


# Print the tables of stocks with big changes
@app.callback(Output('stock-alert-data', 'children'),
              [Input('stock-records', 'children'), Input('perc-dropdown', 'value')]
              )
def plot_alert_stock_df(df_json, perc):
    stock_alert_df = pd.read_json(df_json)
    percf = float(perc)
    stock_alert_df = stock_alert_df[stock_alert_df['Percentage_Change'].abs() > percf]
    stock_alert_df['Percentage_Change'] = pd.Series(
        ["{0:.2f}%".format(val) for val in stock_alert_df['Percentage_Change']], index=stock_alert_df.index)
    user_symbol_list_df = stock_alert_df.copy(deep=True)
    print(user_symbol_list_df)
    user_symbol_list_df.to_csv("user_symbol_list_df.csv")
    return generate_alert_table(stock_alert_df)


# candelsticks for OHLC of historic data
@app.callback(
    dash.dependencies.Output('dd-output-container', 'figure'),
    [dash.dependencies.Input('demo-dropdown', 'value')])
def update_output(value):
    temp_df = historic_table_gcp
    temp_df_stock = temp_df['Symbol'] == value
    temp_df = temp_df[temp_df_stock]

    fig = go.Figure(data=[go.Candlestick(x=temp_df['Date'],
                                         open=temp_df['Open'], high=temp_df['High'],
                                         low=temp_df['Low'], close=temp_df['Close'])
                          ])

    fig.update_layout(xaxis_rangeslider_visible=False)

    return fig


if __name__ == "__main__":
    app.run_server(debug=True, host='0.0.0.0')
