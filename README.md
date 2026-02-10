# Slack Multi Bot

![Python](https://img.shields.io/badge/Python-3.9-3776AB?logo=python&logoColor=white)
![Slack](https://img.shields.io/badge/Slack_Bolt-4A154B?logo=slack&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-000000?logo=flask&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![Amazon Redshift](https://img.shields.io/badge/Amazon_Redshift-8C4FFF?logo=amazonredshift&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=white)

An interactive Slack bot built with **Slack Bolt (Socket Mode)** that provides a button-driven interface for running automated tasks — including data extraction from **Amazon Redshift** and CSV report generation — directly from Slack channels.

---

## Features

- **Interactive Button Menu** — Sends an action menu when a message is received, allowing users to trigger tasks with a single click.
- **Redshift Data Export** — Connects to Amazon Redshift, runs a comprehensive product catalog query (Magento), and uploads the result as a CSV file to the Slack channel.
- **CSV Generation** — Generates and uploads CSV reports on demand.
- **Real-time Message Updates** — Demonstrates live message editing via Slack's `chat_update` API (count-to-ten example).
- **Health Check Endpoint** — Exposes a `/health` route via Flask for service monitoring and deployment platforms (e.g., Render).

## Architecture

```
┌──────────────────────────────────────────────┐
│                  Slack Multi Bot             │
│                                              │
│  ┌──────────────┐    ┌────────────────────┐  │
│  │  Slack Bolt   │    │   Flask Server     │  │
│  │ (Socket Mode) │    │   (Port 3000)      │  │
│  │               │    │                    │  │
│  │ - Button menu │    │ - /health endpoint │  │
│  │ - Actions     │    └────────────────────┘  │
│  │ - Events      │              ▲              │
│  └──────┬───────┘              │              │
│         │               Health checks         │
│         ▼                                     │
│  ┌──────────────┐                             │
│  │  redshift.py  │                             │
│  │               │                             │
│  │ - SQL queries │                             │
│  │ - DataFrame   │                             │
│  │ - CSV export  │                             │
│  └──────┬───────┘                             │
│         │                                     │
└─────────┼─────────────────────────────────────┘
          ▼
   Amazon Redshift
   (Magento catalog)
```

Both the Slack bot and the Flask server run concurrently using Python threads.

## Available Actions

| Action | Description |
|---|---|
| **Search for missing images** | Identifies products with missing image assets |
| **Incorrect sentences** | Flags incorrect sentences in product data |
| **Misspelled words** | Detects misspelled words across catalog entries |
| **Generate CSV** | Creates and uploads a sample CSV report |
| **Generate Redshift CSV** | Extracts product data from Redshift and uploads as CSV |
| **Count to Ten** | Demo action — updates a single message in real time |

## Getting Started

### Prerequisites

- Python 3.9+
- A Slack App with **Socket Mode** enabled
- (Optional) Access to an Amazon Redshift cluster

### Environment Variables

Create a `.env` file in the project root:

```env
# Slack
SLACK_BOT_TOKEN=xoxb-...
SLACK_APP_TOKEN=xapp-...

# OpenAI (reserved for future use)
OPENAI_API_KEY=sk-...

# Amazon Redshift
REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_PORT=5439
REDSHIFT_DBNAME=your_database
REDSHIFT_USER=your_user
REDSHIFT_PASSWORD=your_password
```

> **Note:** The `.env` file is listed in `.gitignore` and should never be committed.

### Installation

```bash
# Clone the repository
git clone https://github.com/Tam-Leal/slack-multi-bot.git
cd slack-multi-bot

# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Running Locally

```bash
python buttons-interaction.py
```

The Slack bot will connect via Socket Mode and the Flask health server will start on port `3000`.

### Running with Docker

```bash
docker build -t slack-multi-bot .
docker run --env-file .env -p 3000:3000 slack-multi-bot
```

## Project Structure

```
slack-multi-bot/
├── buttons-interaction.py   # Main entry point — Slack bot + Flask server
├── redshift.py              # Redshift connection, queries, and data processing
├── requirements.txt         # Python dependencies
├── Dockerfile               # Container configuration
└── .gitignore
```

## License

This project is proprietary and not licensed for public use.
