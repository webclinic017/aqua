# Aqua
A quant trading framework for fetching market data, managing portfolios (in progress), running theo calculations (in progress), and a webserver (in progress)

## Getting started
Install requirements:

```pip install requirements.txt```

Install dev requirements:

```pip install requirements.dev.txt```

Install the aqua package locally (for `pytest`)

`pip install -e .`

### Data sources
A `.env` file is required for configuring API keys and URLs. This is optional but the `market_data` module will not work without these configurations.

Sample:
```
POLYGON_URL="https://api.polygon.io"
POLYGON_API_KEY=<polygon api key>

ALPACA_URL="https://paper-api.alpaca.markets"
ALPACA_DATA_URL="https://data.alpaca.markets"
ALPACA_DATA_WS_URL="wss://stream.data.alpaca.markets"
ALPACA_KEY_ID=<alpaca key>
ALPACA_SECRET_KEY=<alpaca secret key>

TWS_URL="localhost"
TWS_PORT="4001"

AQUA_WEBSERVER_KEY="notprod"
```
