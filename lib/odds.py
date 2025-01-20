import requests
import os

from dotenv import load_dotenv

load_dotenv()


class Odds:
    base_url = "https://api.the-odds-api.com/v4/sports/mma_mixed_martial_arts/"

    def _get(self, endpoint: str, params: dict = {}) -> dict:
        p = params.copy()
        p["apiKey"] = self._api_key
        response = requests.get(Odds.base_url + endpoint.lstrip("/"), params=p)
        if response.status_code != 200:
            print(response.status_code)
            print(response.text)
        return response.json()

    def list_fights(self, start, end) -> dict:
        return self._get(
            "events",
            {
                "commenceTimeFrom": start,
                "commenceTimeTo": end,
            },
        )

    def get_odds(self, fight_id: int) -> dict:
        return self._get(
            "odds",
            {
                "regions": "us,us2",
                "markets": "h2h",
                "oddsFormat": "decimal",
                "eventIds": fight_id,
            },
        )

    def __init__(self, api_key: str = None):
        if api_key is None:
            self._api_key = os.getenv("ODDS_API_KEY")
        else:
            self._api_key = api_key
