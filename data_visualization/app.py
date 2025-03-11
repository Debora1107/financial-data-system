import sys
import os
import json
import requests
import pandas as pd
import plotly.graph_objs as go
import plotly.express as px
from datetime import datetime, timedelta
from dash import Dash, html, dcc, callback, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import numpy as np
import yfinance as yf

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_logger, load_config

logger = get_logger("data_visualization_app")
config = load_config()

INGESTION_URL = f"http://{config['db']['host']}:{config['services']['data_ingestion']['port']}"
ANALYSIS_URL = f"http://{config['db']['host']}:{config['services']['data_analysis']['port']}"
STORAGE_URL = f"http://{config['db']['host']}:{config['services']['data_storage']['port']}"

app = Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
server = app.server

# Get S&P 500 symbols with company names
def get_sp500_symbols():
    try:
        sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(sp500_url)
        df = tables[0]
        symbols_with_names = [{"symbol": row["Symbol"], "name": row["Security"]} 
                            for _, row in df.iterrows()]
        return symbols_with_names
    except Exception as e:
        logger.error(f"Error fetching S&P 500 symbols: {e}")
        return [
            {"symbol": "AAPL", "name": "Apple Inc."},
            {"symbol": "MSFT", "name": "Microsoft Corporation"},
            {"symbol": "GOOG", "name": "Alphabet Inc."},
            {"symbol": "AMZN", "name": "Amazon.com Inc."},
            {"symbol": "META", "name": "Meta Platforms Inc."}
        ]

AVAILABLE_SYMBOLS = get_sp500_symbols()

app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H1("Financial Data Dashboard", className="text-center my-4"),
            html.P("This dashboard visualizes financial data and analysis.", className="text-center mb-4")
        ])
    ]),
    
    # Stock Selection and Price Chart Row
    dbc.Row([
        # Left Column - Stock Selection
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Stock Selection"),
                dbc.CardBody([
                    dbc.Form([
                        dbc.Label("Search Company or Symbol"),
                        dcc.Input(
                            id="symbol-search",
                            type="text",
                            placeholder="Enter company name or symbol...",
                            className="mb-2 form-control"
                        ),
                        dbc.Label("Select Stock"),
                        dcc.Dropdown(
                            id="symbol-dropdown",
                            options=[{
                                "label": f"{s['symbol']} - {s['name']}", 
                                "value": s['symbol']
                            } for s in AVAILABLE_SYMBOLS],
                            value="AAPL",
                            clearable=False,
                            searchable=True,
                            placeholder="Select or type to search..."
                        ),
                        html.Small(
                            "Type to search through all S&P 500 companies",
                            className="text-muted"
                        )
                    ]),
                    dbc.Form([
                        dbc.Label("Time Period"),
                        dcc.Dropdown(
                            id="period-dropdown",
                            options=[
                                {"label": "1 Week", "value": "1w"},
                                {"label": "1 Month", "value": "1mo"},
                                {"label": "3 Months", "value": "3mo"},
                                {"label": "6 Months", "value": "6mo"},
                                {"label": "1 Year", "value": "1y"}
                            ],
                            value="1mo",
                            clearable=False
                        )
                    ]),
                    dbc.Button("Fetch Data", id="fetch-button", color="primary", className="mt-3"),
                    dbc.Spinner(html.Div(id="loading-output"), color="primary", type="grow", size="sm")
                ])
            ])
        ], width=3),
        
        # Right Column - Stock Price Chart
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Stock Price"),
                dbc.CardBody([
                    dcc.Graph(id="price-chart")
                ])
            ])
        ], width=9)
    ], className="mb-4"),
    
    # Technical Indicators Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Technical Indicators"),
                dbc.CardBody([
                    dbc.Tabs([
                        dbc.Tab([
                            dcc.Graph(id="ma-chart")
                        ], label="Moving Averages"),
                        dbc.Tab([
                            dcc.Graph(id="rsi-chart")
                        ], label="RSI"),
                        dbc.Tab([
                            dcc.Graph(id="macd-chart")
                        ], label="MACD"),
                        dbc.Tab([
                            dcc.Graph(id="bb-chart")
                        ], label="Bollinger Bands")
                    ])
                ])
            ], className="mb-4")
        ], width=8),
        
        # Right Column - Sentiment and Prediction
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Sentiment Analysis"),
                dbc.CardBody([
                    html.Div(id="sentiment-content")
                ])
            ], className="mb-4"),
            
            dbc.Card([
                dbc.CardHeader("Price Prediction"),
                dbc.CardBody([
                    dcc.Graph(id="prediction-chart")
                ])
            ], className="mb-4")
        ], width=4)
    ]),
    
    # Summary Row
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Data Summary"),
                dbc.CardBody([
                    html.Div(id="summary-content")
                ])
            ], className="mb-4")
        ])
    ])
], fluid=True)

@app.callback(
    [Output("loading-output", "children"),
     Output("loading-output", "style")],
    [Input("fetch-button", "n_clicks"),
     Input("price-chart", "figure")],
    [State("symbol-dropdown", "value")]
)
def update_loading_state(n_clicks, price_figure, symbol):
    """Update loading state when fetching data."""
    if n_clicks is None:
        return "", {"display": "none"}
    
    # Ако имаме figure и не е празен (има податоци)
    if price_figure and price_figure.get("data", []):
        return f"Податоците за {symbol} се успешно вчитани!", {"color": "green", "margin-top": "10px"}
    
    if ctx.triggered_id == "fetch-button":
        return f"Се вчитуваат податоци за {symbol}...", {"color": "blue", "margin-top": "10px"}
    
    return "", {"display": "none"}

@app.callback(
    Output("symbol-dropdown", "options"),
    Input("symbol-search", "value")
)
def update_symbol_options(search_value):
    """Update symbol dropdown options based on search input."""
    if not search_value:
        return [{
            "label": f"{s['symbol']} - {s['name']}", 
            "value": s['symbol']
        } for s in AVAILABLE_SYMBOLS]
    
    search_value = search_value.upper()
    filtered_symbols = [
        s for s in AVAILABLE_SYMBOLS 
        if search_value in s['symbol'].upper() or search_value in s['name'].upper()
    ]
    return [{
        "label": f"{s['symbol']} - {s['name']}", 
        "value": s['symbol']
    } for s in filtered_symbols]

@app.callback(
    [
        Output("price-chart", "figure"),
        Output("ma-chart", "figure"),
        Output("rsi-chart", "figure"),
        Output("macd-chart", "figure"),
        Output("bb-chart", "figure"),
        Output("sentiment-content", "children"),
        Output("prediction-chart", "figure"),
        Output("summary-content", "children")
    ],
    [Input("fetch-button", "n_clicks")],
    [
        State("symbol-dropdown", "value"),
        State("period-dropdown", "value")
    ],
    prevent_initial_call=True  # Спречува иницијално повикување
)
def update_dashboard(n_clicks, symbol, period):
    """Update dashboard with data for the selected symbol and period."""
    if n_clicks is None:
        raise dash.exceptions.PreventUpdate
    
    try:
        # Try to get real data from Yahoo Finance
        try:
            ticker = yf.Ticker(symbol)
            hist = ticker.history(period=period)
            if not hist.empty:
                df = hist.copy()
                df.reset_index(inplace=True)
                df.rename(columns={'Date': 'date', 'Close': 'close'}, inplace=True)
                
                # Calculate indicators
                df['ma5'] = df['close'].rolling(window=5).mean()
                df['ma20'] = df['close'].rolling(window=20).mean()
                df['daily_return'] = df['close'].pct_change()
                df['volatility'] = df['daily_return'].rolling(window=20).std()
                
                # RSI
                delta = df['close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
                rs = gain / loss
                df['rsi'] = 100 - (100 / (1 + rs))
                
                # Bollinger Bands
                df['bb_middle'] = df['close'].rolling(window=20).mean()
                std = df['close'].rolling(window=20).std()
                df['bb_upper'] = df['bb_middle'] + (std * 2)
                df['bb_lower'] = df['bb_middle'] - (std * 2)
                
                # MACD
                exp1 = df['close'].ewm(span=12, adjust=False).mean()
                exp2 = df['close'].ewm(span=26, adjust=False).mean()
                df['macd'] = exp1 - exp2
                df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
                df['macd_histogram'] = df['macd'] - df['macd_signal']
                
                indicators_data = df.to_dict('records')
            else:
                raise Exception("No data available")
        except Exception as e:
            logger.error(f"Error fetching data from Yahoo Finance: {e}")
            indicators_data = generate_sample_data(symbol, period)
        
        df = pd.DataFrame(indicators_data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        
        price_fig = create_price_chart(df, symbol)
        ma_fig = create_ma_chart(df, symbol)
        rsi_fig = create_rsi_chart(df, symbol)
        macd_fig = create_macd_chart(df, symbol)
        bb_fig = create_bb_chart(df, symbol)
        
        sentiment_data = generate_sample_sentiment(symbol)
        sentiment_content = create_sentiment_content(sentiment_data)
        
        prediction_data = generate_sample_prediction(symbol, df["close"].iloc[-1])
        prediction_fig = create_prediction_chart(prediction_data, symbol)
        
        summary_data = generate_sample_summary(symbol, df["close"].iloc[-1])
        summary_content = create_summary_content(summary_data)
        
        return price_fig, ma_fig, rsi_fig, macd_fig, bb_fig, sentiment_content, prediction_fig, summary_content
    except Exception as e:
        logger.error(f"Error updating dashboard: {e}")
        empty_fig = go.Figure()
        empty_fig.update_layout(
            title="Грешка при вчитување на податоците",
            annotations=[{
                "text": "Обидете се повторно или изберете друга компанија",
                "xref": "paper",
                "yref": "paper",
                "showarrow": False,
                "font": {"size": 14}
            }]
        )
        empty_content = html.Div([
            html.H4("Грешка при вчитување", className="text-danger"),
            html.P("Обидете се повторно или изберете друга компанија")
        ])
        return empty_fig, empty_fig, empty_fig, empty_fig, empty_fig, empty_content, empty_fig, empty_content

def create_price_chart(df, symbol):
    """Create a price chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["close"],
        mode="lines",
        name="Close Price",
        line=dict(color="blue")
    ))
    
    fig.update_layout(
        title=f"{symbol} Stock Price",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_white"
    )
    
    return fig

def create_ma_chart(df, symbol):
    """Create a moving averages chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["close"],
        mode="lines",
        name="Close Price",
        line=dict(color="blue")
    ))
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["ma5"],
        mode="lines",
        name="5-Day MA",
        line=dict(color="orange")
    ))
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["ma20"],
        mode="lines",
        name="20-Day MA",
        line=dict(color="red")
    ))
    
    fig.update_layout(
        title=f"{symbol} Moving Averages",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_white"
    )
    
    return fig

def create_rsi_chart(df, symbol):
    """Create an RSI chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["rsi"],
        mode="lines",
        name="RSI",
        line=dict(color="purple")
    ))
    
    fig.add_shape(
        type="line",
        x0=df["date"].iloc[0],
        y0=70,
        x1=df["date"].iloc[-1],
        y1=70,
        line=dict(color="red", width=1, dash="dash")
    )
    
    fig.add_shape(
        type="line",
        x0=df["date"].iloc[0],
        y0=30,
        x1=df["date"].iloc[-1],
        y1=30,
        line=dict(color="green", width=1, dash="dash")
    )
    
    fig.update_layout(
        title=f"{symbol} Relative Strength Index (RSI)",
        xaxis_title="Date",
        yaxis_title="RSI",
        yaxis=dict(range=[0, 100]),
        template="plotly_white"
    )
    
    return fig

def create_macd_chart(df, symbol):
    """Create a MACD chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["macd"],
        mode="lines",
        name="MACD",
        line=dict(color="blue")
    ))
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["macd_signal"],
        mode="lines",
        name="Signal Line",
        line=dict(color="red")
    ))
    
    fig.add_trace(go.Bar(
        x=df["date"],
        y=df["macd_histogram"],
        name="Histogram",
        marker=dict(
            color=df["macd_histogram"].apply(lambda x: "green" if x > 0 else "red")
        )
    ))
    
    fig.update_layout(
        title=f"{symbol} MACD",
        xaxis_title="Date",
        yaxis_title="Value",
        template="plotly_white"
    )
    
    return fig

def create_bb_chart(df, symbol):
    """Create a Bollinger Bands chart."""
    fig = go.Figure()
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["close"],
        mode="lines",
        name="Close Price",
        line=dict(color="blue")
    ))
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["bb_upper"],
        mode="lines",
        name="Upper Band",
        line=dict(color="red")
    ))
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["bb_middle"],
        mode="lines",
        name="Middle Band",
        line=dict(color="orange")
    ))
    
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["bb_lower"],
        mode="lines",
        name="Lower Band",
        line=dict(color="green")
    ))
    
    fig.update_layout(
        title=f"{symbol} Bollinger Bands",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_white"
    )
    
    return fig

def create_sentiment_content(sentiment_data):
    """Create sentiment analysis content."""
    sentiment = sentiment_data.get("sentiment", "neutral")
    price = sentiment_data.get("price", 0)
    change_percent = sentiment_data.get("change_percent", 0)
    signals = sentiment_data.get("signals", {})
    
    color = "secondary"
    if sentiment == "bullish":
        color = "success"
    elif sentiment == "bearish":
        color = "danger"
    
    content = [
        html.H4(f"{sentiment_data.get('symbol', '')} - ${price:.2f}", className="mb-2"),
        html.P([
            html.Span(f"{change_percent:.2f}%", className=f"text-{'success' if change_percent >= 0 else 'danger'}"),
            " today"
        ], className="mb-3"),
        
        html.H5("Overall Sentiment", className="mb-2"),
        dbc.Badge(sentiment.upper(), color=color, className="mb-3 p-2"),
        
        html.H5("Technical Signals", className="mb-2"),
        dbc.ListGroup([
            dbc.ListGroupItem([
                html.Div("Price Trend", className="fw-bold"),
                dbc.Badge(signals.get("price_trend", "neutral").upper(), color=get_signal_color(signals.get("price_trend", "neutral")))
            ]),
            dbc.ListGroupItem([
                html.Div("RSI", className="fw-bold"),
                html.Div(f"Value: {signals.get('rsi', {}).get('value', 0):.2f}"),
                dbc.Badge(signals.get("rsi", {}).get("signal", "neutral").upper(), color=get_signal_color(signals.get("rsi", {}).get("signal", "neutral")))
            ]),
            dbc.ListGroupItem([
                html.Div("MACD", className="fw-bold"),
                dbc.Badge(signals.get("macd", {}).get("trend", "neutral").upper(), color=get_signal_color(signals.get("macd", {}).get("trend", "neutral")))
            ]),
            dbc.ListGroupItem([
                html.Div("Bollinger Bands", className="fw-bold"),
                html.Div(f"Position: {signals.get('bollinger_bands', {}).get('position', 0):.2f}"),
                dbc.Badge(signals.get("bollinger_bands", {}).get("signal", "neutral").upper(), color=get_signal_color(signals.get("bollinger_bands", {}).get("signal", "neutral")))
            ])
        ])
    ]
    
    return content

def create_prediction_chart(prediction_data, symbol):
    """Create a prediction chart."""
    fig = go.Figure()
    
    predictions = prediction_data.get("predictions", [])
    
    if not predictions:
        fig.update_layout(title="No prediction data available")
        return fig
    
    dates = [datetime.fromisoformat(p["date"]) for p in predictions]
    lr_values = [p["linear_regression"] for p in predictions]
    arima_values = [p["arima"] for p in predictions]
    avg_values = [p["average"] for p in predictions]
    
    last_price = prediction_data.get("last_price", 0)
    last_date = dates[0] - timedelta(days=1)
    
    dates = [last_date] + dates
    lr_values = [last_price] + lr_values
    arima_values = [last_price] + arima_values
    avg_values = [last_price] + avg_values
    
    fig.add_trace(go.Scatter(
        x=dates,
        y=lr_values,
        mode="lines",
        name="Linear Regression",
        line=dict(color="blue")
    ))
    
    fig.add_trace(go.Scatter(
        x=dates,
        y=arima_values,
        mode="lines",
        name="ARIMA",
        line=dict(color="green")
    ))
    
    fig.add_trace(go.Scatter(
        x=dates,
        y=avg_values,
        mode="lines",
        name="Average",
        line=dict(color="red", width=3)
    ))
    
    fig.update_layout(
        title=f"{symbol} Price Prediction (Next 5 Days)",
        xaxis_title="Date",
        yaxis_title="Price ($)",
        template="plotly_white"
    )
    
    return fig

def create_summary_content(summary_data):
    """Create summary content."""
    symbol = summary_data.get("symbol", "")
    current_price = summary_data.get("current_price", 0)
    change_percent = summary_data.get("change_percent", 0)
    sentiment = summary_data.get("sentiment", "neutral")
    prediction = summary_data.get("prediction", {})
    
    tomorrow = prediction.get("tomorrow", {})
    tomorrow_avg = tomorrow.get("average", current_price) if tomorrow else current_price
    tomorrow_change = ((tomorrow_avg - current_price) / current_price) * 100 if current_price else 0
    
    content = dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Current Price"),
                dbc.CardBody([
                    html.H3(f"${current_price:.2f}"),
                    html.P([
                        html.Span(f"{change_percent:.2f}%", className=f"text-{'success' if change_percent >= 0 else 'danger'}"),
                        " today"
                    ])
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Sentiment"),
                dbc.CardBody([
                    html.H3(sentiment.upper()),
                    dbc.Badge(sentiment.upper(), color=get_signal_color(sentiment), className="p-2")
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Tomorrow's Prediction"),
                dbc.CardBody([
                    html.H3(f"${tomorrow_avg:.2f}"),
                    html.P([
                        html.Span(f"{tomorrow_change:.2f}%", className=f"text-{'success' if tomorrow_change >= 0 else 'danger'}"),
                        " expected"
                    ])
                ])
            ])
        ], width=3),
        
        dbc.Col([
            dbc.Card([
                dbc.CardHeader("Recommendation"),
                dbc.CardBody([
                    html.H3(get_recommendation(sentiment, tomorrow_change)),
                    dbc.Badge(get_recommendation(sentiment, tomorrow_change), color=get_recommendation_color(sentiment, tomorrow_change), className="p-2")
                ])
            ])
        ], width=3)
    ])
    
    return content

def get_signal_color(signal):
    """Get color for a signal."""
    if signal in ["bullish", "oversold"]:
        return "success"
    elif signal in ["bearish", "overbought"]:
        return "danger"
    else:
        return "secondary"

def get_recommendation(sentiment, tomorrow_change):
    """Get recommendation based on sentiment and tomorrow's change."""
    if sentiment == "bullish" and tomorrow_change > 0:
        return "BUY"
    elif sentiment == "bearish" and tomorrow_change < 0:
        return "SELL"
    elif sentiment == "neutral" and abs(tomorrow_change) < 1:
        return "HOLD"
    elif tomorrow_change > 2:
        return "BUY"
    elif tomorrow_change < -2:
        return "SELL"
    else:
        return "HOLD"

def get_recommendation_color(sentiment, tomorrow_change):
    """Get color for a recommendation."""
    recommendation = get_recommendation(sentiment, tomorrow_change)
    if recommendation == "BUY":
        return "success"
    elif recommendation == "SELL":
        return "danger"
    else:
        return "warning"

def generate_sample_data(symbol, period):
    """Generate sample data for testing."""
    days = 30
    if period == "1w":
        days = 7
    elif period == "1mo":
        days = 30
    elif period == "3mo":
        days = 90
    elif period == "6mo":
        days = 180
    elif period == "1y":
        days = 365
    
    data = []
    base_date = datetime.now() - timedelta(days=days)
    base_price = 150.0
    
    for i in range(days):
        date = base_date + timedelta(days=i)
        close_price = base_price + np.random.normal(0, 2)
        data.append({
            "date": date.isoformat(),
            "close": close_price,
            "ma5": close_price + np.random.normal(0, 1),
            "ma20": close_price + np.random.normal(0, 2),
            "daily_return": np.random.normal(0, 0.01),
            "volatility": np.random.uniform(0.01, 0.03),
            "rsi": np.random.uniform(30, 70),
            "bb_middle": close_price,
            "bb_upper": close_price + np.random.uniform(5, 10),
            "bb_lower": close_price - np.random.uniform(5, 10),
            "macd": np.random.normal(0, 1),
            "macd_signal": np.random.normal(0, 1),
            "macd_histogram": np.random.normal(0, 0.5)
        })
        base_price = close_price
    
    return data

def generate_sample_sentiment(symbol):
    """Generate sample sentiment data for testing."""
    return {
        "symbol": symbol,
        "price": 153.0,
        "change_percent": 0.33,
        "sentiment": np.random.choice(["bullish", "bearish", "neutral"]),
        "signals": {
            "price_trend": np.random.choice(["bullish", "bearish", "neutral"]),
            "rsi": {
                "value": np.random.uniform(30, 70),
                "signal": np.random.choice(["overbought", "oversold", "neutral"])
            },
            "macd": {
                "value": np.random.normal(0, 1),
                "signal": np.random.normal(0, 1),
                "trend": np.random.choice(["bullish", "bearish", "neutral"])
            },
            "bollinger_bands": {
                "position": np.random.uniform(0, 1),
                "signal": np.random.choice(["overbought", "oversold", "neutral"])
            }
        }
    }

def generate_sample_prediction(symbol, last_price):
    """Generate sample prediction data for testing."""
    predictions = []
    base_date = datetime.now()
    
    for i in range(5):
        date = base_date + timedelta(days=i+1)
        lr_value = last_price + np.random.normal(0, 2) + (i * 0.5)
        arima_value = last_price + np.random.normal(0, 2) + (i * 0.3)
        predictions.append({
            "date": date.isoformat(),
            "linear_regression": lr_value,
            "arima": arima_value,
            "average": (lr_value + arima_value) / 2
        })
    
    return {
        "symbol": symbol,
        "last_price": last_price,
        "predictions": predictions
    }

def generate_sample_summary(symbol, current_price):
    """Generate sample summary data for testing."""
    sentiment = np.random.choice(["bullish", "bearish", "neutral"])
    tomorrow_change = np.random.normal(0, 2)
    
    return {
        "symbol": symbol,
        "current_price": current_price,
        "change_percent": np.random.normal(0, 1),
        "sentiment": sentiment,
        "prediction": {
            "tomorrow": {
                "average": current_price * (1 + (tomorrow_change / 100))
            }
        }
    }

if __name__ == "__main__":
    port = config["services"]["data_visualization"]["port"]
    app.run_server(host="0.0.0.0", port=port, debug=True) 