from datetime import datetime
import requests


class PrometheusClient:
    api_url: str

    def __init__(self, host):
        self.api_url = f"http://{host}/api/v1/query_range"

    def get_record_consumed_rates(self, job: str, start_ts: float, end_ts: float) -> list[list[tuple[datetime, float]]]:
        params = {
            'query': f"sum(kafka_consumer_fetch_manager_records_consumed_rate{{job=\"{job}\"}}) by (instance)",
            "start": start_ts,
            "end": end_ts,
            "step": "15s"
        }
        response = requests.get(self.api_url, params=params)
        response.raise_for_status()
        data = response.json()

        result: list[list[tuple[datetime, float]]] = []
        for series in data['data']['result']:
            series_points = [
                (datetime.fromtimestamp(float(point[0])), float(point[1]))
                for point in series['values']
            ]
            result.append(series_points)

        print(f"Prometheus query result for job {job} between {start_ts} and {end_ts}: {len(result)} series")
        return result

    def get_rebalance_timestamps(self, job: str, start_ts: float, end_ts: float) -> list[datetime]:
        params = {
            'query': f"sum(increase(kafka_consumer_coordinator_rebalance_total{{job=\"{job}\"}}[1m]))",
            "start": start_ts,
            "end": end_ts,
            "step": "15s"
        }
        response = requests.get(self.api_url, params=params)
        response.raise_for_status()
        data = response.json()

        times: list[datetime] = []

        for series in data['data']['result']:
            for point in series['values']:
                if float(point[1]) > 0:
                    times.append(datetime.fromtimestamp(float(point[0])))

        print(f"Prometheus rebalance query result between {start_ts} and {end_ts}: {times}")
        return times
