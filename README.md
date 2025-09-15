# ERCOT RTM Sentiment Project

This project collects ERCOT real-time market (RTM) data, computes a consumer sentiment index, and exposes the results through a Streamlit dashboard and optional FastAPI service. Data is stored in an append-only Parquet file (`data/ercot.parquet`) with hourly updates and automatic deduplication.

## Features

- **Automated collector** using the [`gridstatus`](https://github.com/kmax12/gridstatus) library to pull RTM price and system load data from ERCOT each hour.
- **Sentiment engine** that compares price and load to a 7-day rolling baseline using robust statistics and weighted scoring.
- **Streamlit + Plotly dashboard** for interactive exploration with zone selection, timezone toggle, KPI cards, and charts for price, load, and sentiment.
- **FastAPI microservice** providing `/latest` and `/history` endpoints for programmatic access.
- **Parquet storage** with append + deduplicate logic to keep the dataset tidy.
- **Automation-ready** GitHub Actions workflow that can run hourly and commit refreshed data.
- **Dockerized deployment** targeting the Streamlit app by default.

## Project Structure

```
app/
  api.py            # FastAPI service
  fetch.py          # gridstatus integrations
  locations.py      # zone/hub normalization utilities
  sentiment.py      # sentiment scoring logic
  storage.py        # parquet helpers
collector.py        # CLI collector entrypoint
streamlit_app.py    # Streamlit dashboard
requirements.txt    # Python dependencies
Dockerfile          # Container definition for the dashboard
.github/workflows/hourly.yml  # GitHub Actions workflow
```

## Getting Started Locally

1. **Create a virtual environment** (recommended):
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

2. **Install dependencies**:
   ```bash
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. **Run the collector** to populate `data/ercot.parquet`:
   ```bash
   python collector.py --lookback 168  # fetch last 7 days on first run
   ```

   Subsequent runs can use the default 48-hour lookback to refresh recent history.

4. **Launch the Streamlit dashboard**:
   ```bash
   streamlit run streamlit_app.py
   ```

   The app listens on <http://localhost:8501> by default. Select a zone or hub, adjust the timezone, and explore the charts.

5. **Start the FastAPI service (optional)**:
   ```bash
   uvicorn app.api:app --reload --port 8000
   ```

   - `GET /latest?zone=NORTH` returns the latest row for the specified zone/hub.
   - `GET /history?zone=HB_HOUSTON&hours=72` returns the last 72 hours for that location (capped at 14 days).

## Scheduling & Automation

- **Local cron** example (run every hour):
  ```cron
  0 * * * * /usr/bin/env bash -c 'cd /path/to/project && source .venv/bin/activate && python collector.py --lookback 6 >> collector.log 2>&1'
  ```

- **GitHub Actions**: The workflow at `.github/workflows/hourly.yml` runs hourly, executes the collector, commits the updated Parquet file, and pushes the change back to the repository when data changes are detected.

## Deployment Options

### Docker

Build and run the containerized Streamlit app:

```bash
docker build -t ercot-rtm .
docker run -p 8501:8501 -v $(pwd)/data:/app/data ercot-rtm
```

Mounting the `data/` directory preserves the Parquet history across restarts.

### Streamlit Community Cloud

1. Push this repository to GitHub.
2. In Streamlit Cloud, create a new app pointing to `streamlit_app.py`.
3. Configure secrets/environment variables if needed (e.g., GitHub token for the workflow).
4. Ensure the `data/` directory is persisted via an external storage solution if long-term history is required.

### FastAPI Hosting

Deploy the FastAPI service (e.g., on Railway, Fly.io, or Azure App Service) by running `uvicorn app.api:app --host 0.0.0.0 --port 8000`. Mount or synchronize the `data/ercot.parquet` file to keep the API in sync with the collector.

## Development Tips

- The collector logs to stdout. Use the `--verbose` flag for detailed logging when troubleshooting.
- Sentiment thresholds: green ≥ 70, yellow 40–69, red < 40.
- Rolling baselines use 7 days (168 hours) of history with at least 24 observations before scoring.

## License

This project is released under the [MIT License](LICENSE).
