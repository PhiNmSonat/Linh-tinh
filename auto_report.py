

import math
import scrapy
import json
from urllib.parse import urlencode
import os
from datetime import datetime, timedelta


class WebscrapeSpider(scrapy.Spider):
    name = "authenticated_spider"
    allowed_domains = ["bi-apis.sonatgame.com"]

    # Define base URL
    base_url = "https://bi-apis.sonatgame.com/"

    # List of URL paths to iterate through
    paths = [
        "apps/release",
        "levels/dropoff",
        "engagement/engagement-time",
        "monetization/day-show-inters",
        "monetization/day-rewarded-video",
        "monetization/total-revenue-date",
        "monetization/average-revenue"
    ]

    # Load configuration from config.json
    def __init__(self, *args, **kwargs):
        super(WebscrapeSpider, self).__init__(*args, **kwargs)
        self.load_config()

    def load_config(self):
        config_path = "config.json"
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, 'r') as f:
            config = json.load(f)

        self.default_params = config.get('default_params', {})
        self.countries = config.get('countries', [])
        self.appIDs = config.get('appIDs', [])  # Load appIDs from config



    token_file='bearer_token.txt'
    if os.path.exists(token_file):
        with open(token_file, 'r') as f:
            bearer_token=f.read().strip()
    else:
        raise FileNotFoundError("Bearer token file not found")
    # Update with your bearer token
    # bearer_token = 'bearer_token.txt'
    headers = {
        'Accept': '*/*',
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json',
        'User-Agent': 'Mozilla/5.0'
    }

    def start_requests(self):
        for app_id in self.appIDs:
            self.default_params['appId'] = app_id  # Set appId for each request
            for path in self.paths:
                # Request with default params only (no country)
                query_string = urlencode(self.default_params)
                url = f"{self.base_url}{path}?{query_string}"
                yield scrapy.Request(
                    url=url,
                    headers=self.headers,
                    callback=self.parse_json,
                    meta={'path': path, 'country': None, 'appId': app_id}
                )
                for country in self.countries:
                    params = self.default_params.copy()
                    params['country'] = country
                    query_string = urlencode(params)
                    url = f"{self.base_url}{path}?{query_string}"
                    yield scrapy.Request(
                        url=url,
                        headers=self.headers,
                        callback=self.parse_json,
                        meta={'path': path, 'country': country, 'appId': app_id}
                    )

            # Add POST request for "leaderboard/details/retention-rate"
            post_path = "leaderboard/details/retention-rate"
            for country in [None] + self.countries:
                # Calculate 'from' date as one day earlier than startDate
                start_date = datetime.strptime(self.default_params['startDate'], '%Y-%m-%d')
                from_date = (start_date - timedelta(days=1)).strftime('%Y-%m-%d')
                payload = {
                    'appId': app_id,
                    'from': from_date,
                    'to': self.default_params['endDate'],
                    'filters': {'geo': []},
                    'groupings': ['date']
                }
                # Country to country code mapping
                country_code_mapping = {
                    'United Kingdom': 'GB',
                    'United States': 'US',
                    'Germany': 'DE',
                    'Italy': 'IT',
                    'Japan': 'JP',
                    'South Korea': 'KR',
                    'Mexico': 'MX',
                    'India': 'IN',
                    'France': 'FR',
                    'Russia': 'RU',
                    'Belgium': 'BE',
                    'Brazil': 'BR'
                }

                if country is None:
                    country_name = "Global"
                    payload['filters']['geo'] = []
                else:
                    country_name = country
                    country_code = country_code_mapping.get(country)
                    if country_code:
                        payload['filters']['geo'] = [country_code]
                    else:
                        self.logger.error(f"Country code for {country} not found.")

                url = f"{self.base_url}{post_path}?appId={app_id}"

                yield scrapy.FormRequest(
                    url=url,
                    method='POST',
                    headers=self.headers,
                    body=json.dumps(payload),
                    callback=self.parse_post_json,
                    meta={'path': post_path, 'country': country_name, 'appId': app_id}
                )

    #########################
    def parse_json(self, response):
        if response.status == 200:
            try:
                data = json.loads(response.text)
                if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                    path = response.meta['path']
                    country = response.meta['country']
                    app_id = response.meta['appId']  # Changed from app_Id to app_id for consistency

                    # Perform calculations
                    results = self.perform_calculations(path, data)
                    results['appId'] = app_id

                    # Save calculation results to output.json
                    self.save_calculation_results(results, country, app_id)  # Added app_id here
                else:
                    self.logger.error("Unexpected JSON format.")
            except json.JSONDecodeError:
                self.logger.error("Failed to decode JSON. Response body might not be valid JSON.")
                self.logger.info(f"Response body: {response.text}")
        else:
            self.logger.error(f"Request failed with status {response.status}")


    def parse_post_json(self, response):
        # Check if the response status code is 201
        if response.status == 201:
            try:
                data = json.loads(response.text)
                country = response.meta.get('country', 'Global')  # Changed 'global' to 'Global' for consistency
                app_id = response.meta['appId']  # Changed from app_Id to app_id for consistency

                # Perform calculations
                retention_rates = self.parse_retention_rate(data)
                retention_rates['appId'] = app_id

                # Save calculations to a separate file
                self.save_calculation_results(retention_rates, country, app_id)  # Added app_id here

            except json.JSONDecodeError:
                self.logger.error("Failed to decode JSON. Response body might not be valid JSON.")
                self.logger.info(f"Response body: {response.text}")



    def perform_calculations(self, path, data):
        # Perform calculations based on the path and return results
        if path == "apps/release":
            return self.parse_app_release(data)
        if path == "engagement/engagement-time":
            return self.parse_engagement_time(data)
        elif path == "levels/dropoff":
            return self.parse_levels_dropoff(data)
        elif path == "engagement/engagement-time":
            return self.parse_day_show_inters(data)
        elif path == "monetization/day-show-inters":
            return self.parse_day_show_inters(data)
        elif path == "monetization/day-rewarded-video":
            return self.parse_day_rewarded_video(data)
        elif path == "monetization/total-revenue-date":
            return self.parse_total_revenue_date(data)
        elif path == "monetization/average-revenue":
            return self.parse_average_revenue(data)
        else:
            self.logger.error(f"No calculations defined for path: {path}")
            return {}

    def save_calculation_results(self, results, country, app_id):
        output_path = "output.json"

        if not os.path.exists(output_path):
            all_results = {}
        else:
            with open(output_path, 'r', encoding='utf-8') as f:
                all_results = json.load(f)

        # Organize results by appId
        if app_id not in all_results:
            all_results[app_id] = {}

        # Save results under the correct country (or Global)
        if country:
            if country not in all_results[app_id]:
                all_results[app_id][country] = {}
            all_results[app_id][country].update(results)
        else:
            if 'Global' not in all_results[app_id]:
                all_results[app_id]['Global'] = {}
            all_results[app_id]['Global'].update(results)

        # Save back to the output file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=4, ensure_ascii=False)

    def parse_app_release(self, data):
        if data:
            app_version = data[0].get("appVersion")
            build_date = data[0].get("buildDate")
            notes = data[0].get("notes")
            return {
                "appVersion": app_version,
                "buildDate": build_date,
                "notes": notes
            }
        return {}
    
    def parse_levels_dropoff(self, data):
        level_data = {
            '0': {'drop_rate': None},
            '20': {'remain_rate': None},
            '50': {'remain_rate': None},
            '100': {'remain_rate': None}
        }
        
        for item in data:
            level = item.get("level")
            if level in level_data:
                if 'drop_rate' in item:
                    level_data[level]['drop_rate'] = item.get("drop_rate")
                if 'remain_rate' in item:
                    level_data[level]['remain_rate'] = item.get("remain_rate")

        return level_data
    
    def parse_engagement_time(self, data):
        engagement_time = [
            item.get("avg_engagement_time_per_dau") for item in data if item.get("avg_engagement_time_per_dau", 0) > 0 
        ]
        avg_engagement_time = sum(engagement_time) / len(engagement_time) if engagement_time else 0
        new_users_engagement_time = [
            item.get("avg_engagement_time_per_dau_new_users") for item in data if item.get("avg_engagement_time_per_dau_new_users", 0) > 0
        ]
        avg_new_users_engagement_time = sum(new_users_engagement_time) / len(new_users_engagement_time) if new_users_engagement_time else 0

        old_users_engagement_time= [
            item.get("avg_engagement_time_per_dau_old_users") for item in data if item.get("avg_engagement_time_per_dau_old_users", 0) > 0
        ]
        avg_old_users_engagement_time = sum(old_users_engagement_time) / len(old_users_engagement_time) if old_users_engagement_time else 0

        return {
            "avg_engagement_time": avg_engagement_time,
            "avg_new_users_engagement_time": avg_new_users_engagement_time,
            "avg_old_users_engagement_time": avg_old_users_engagement_time
        }


    def parse_day_show_inters(self, data):
        impressions_per_dau = [
            item.get("impressions_per_dau") for item in data if item.get("impressions_per_dau", 0) > 0
        ]
        avg_impressions_per_dau = sum(impressions_per_dau) / len(impressions_per_dau) if impressions_per_dau else 0

        new_users_impressions = [
            item.get("new_users_impressions_per_dau") for item in data if item.get("new_users_impressions_per_dau", 0) > 0
        ]
        avg_new_users_impressions = sum(new_users_impressions) / len(new_users_impressions) if new_users_impressions else 0

        old_users_impressions = [
            item.get("old_users_impressions_per_dau") for item in data if item.get("old_users_impressions_per_dau", 0) > 0
        ]
        avg_old_users_impressions = sum(old_users_impressions) / len(old_users_impressions) if old_users_impressions else 0

        return {
            "avg_imp/dau": avg_impressions_per_dau,
            "avg_new_users_imp/dau": avg_new_users_impressions,
            "avg_old_users_imp/dau": avg_old_users_impressions
        }

    def parse_day_rewarded_video(self, data):
        reward_impressions_per_dau = [
            item.get("impressions_per_dau") for item in data if item.get("impressions_per_dau", 0) > 0
        ]
        avg_reward_impressions_per_dau = sum(reward_impressions_per_dau) / len(reward_impressions_per_dau) if reward_impressions_per_dau else 0

        new_users_reward_impressions = [
            item.get("new_users_impressions_per_dau") for item in data if item.get("new_users_impressions_per_dau", 0) > 0
        ]
        avg_new_users_reward_impressions = sum(new_users_reward_impressions) / len(new_users_reward_impressions) if new_users_reward_impressions else 0

        old_users_reward_impressions = [
            item.get("old_users_impressions_per_dau") for item in data if item.get("old_users_impressions_per_dau", 0) > 0
        ]
        avg_old_users_reward_impressions = sum(old_users_reward_impressions) / len(old_users_reward_impressions) if old_users_reward_impressions else 0

        return {
            "avg_rwd/dau": avg_reward_impressions_per_dau,
            "avg_new_users_rwd/dau": avg_new_users_reward_impressions,
            "avg_old_users_rwd/dau": avg_old_users_reward_impressions
        }

    def parse_total_revenue_date(self, data):
        iap_revenue = [
            item.get("iap_revenue") for item in data if item.get("iap_revenue", 0) > 0
        ]
        avg_iap_revenue = sum(iap_revenue) / len(iap_revenue) if iap_revenue else 0
        return {"iap_rev": avg_iap_revenue}

    def parse_average_revenue(self, data):
        paying_users = [
            item.get("paying_users") for item in data if item.get("paying_users", 0) > 0
        ]
        avg_paying_users = sum(paying_users) / len(paying_users) if paying_users else 0

        arppu = [
            item.get("arppu") for item in data if item.get("arppu", 0) > 0
        ]
        avg_arppu = sum(arppu) / len(arppu) if arppu else 0

        return {
            "PU": avg_paying_users,
            "ARPPU": avg_arppu
        }
    def parse_retention_rate(self, data):
        total_d0_for_d1 = 0
        total_d1 = 0
        total_d0_for_d7 = 0
        total_d7 = 0

        for item in data:
            metric_value = item.get('metricValue', {})
            d0 = metric_value.get('d0', 0)
            d1 = metric_value.get('d1', 0)
            d7 = metric_value.get('d7', 0)

            if d1 > 0:
                total_d1 += d1
                total_d0_for_d1 += d0
            
            if d7 > 0:
                total_d7 += d7
                total_d0_for_d7 += d0

        avg_rr_d1 = (total_d1 / total_d0_for_d1) if total_d0_for_d1 > 0 else 0
        avg_rr_d7 = (total_d7 / total_d0_for_d7) if total_d0_for_d7 > 0 else 0

        return {
            'avg_rr_d1': avg_rr_d1,
            'avg_rr_d7': avg_rr_d7
        }


