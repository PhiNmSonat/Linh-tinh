

import json
import pandas as pd
import os

def convert_json_to_excel(json_file, excel_file):
    # Read JSON data
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Prepare a list to hold the rows
    rows = []

    for app_id, countries_data in data.items():
        for country, values in countries_data.items():
            # Consolidate last update information into a single cell
            last_update = f'{values.get("appVersion", "")}\n{values.get("buildDate", "")}\nNotes: {", ".join(values.get("notes", []))}'

            # Consolidate engagement time into a single cell
            engagement_time = f'{values.get("avg_engagement_time", 0)}\n- new: {values.get("avg_new_users_engagement_time", 0)}\n- old: {values.get("avg_old_users_engagement_time", 0)}'

            # Consolidate impressions per DAU into a single cell
            impressions_per_dau = f'{values.get("avg_imp/dau", 0)}\n- new: {values.get("avg_new_users_imp/dau", 0)}\n- old: {values.get("avg_old_users_imp/dau", 0)}'
            reward_impressions_per_dau = f'{values.get("avg_rwd/dau", 0)}\n- new: {values.get("avg_new_users_rwd/dau", 0)}\n- old: {values.get("avg_old_users_rwd/dau", 0)}'

            row = {
                "appID": app_id,
                "Country": country if country else "Global",
                "Last Update": last_update,
                "engagement time": engagement_time,
                "level 0 drop rate": values.get("0", {}).get("drop_rate", ""),
                "level 20 remain rate": values.get("20", {}).get("remain_rate", ""),
                "level 50 remain rate": values.get("50", {}).get("remain_rate", ""),
                "level 100 remain rate": values.get("100", {}).get("remain_rate", ""),
                "impressions per dau (total, new, old)": impressions_per_dau,
                "reward impressions per dau (total, new, old)": reward_impressions_per_dau,
                "iap_rev": values.get("iap_rev", ""),
                "RR D1": values.get("avg_rr_d1", ""),
                "RR D7": values.get("avg_rr_d7", ""),
                "PU": values.get("PU", ""),
                "ARPPU": values.get("ARPPU", "")
            }
            rows.append(row)

    # Convert the list of rows to a DataFrame
    df = pd.DataFrame(rows)

    # Save the DataFrame to an Excel file
    df.to_excel(excel_file, index=False, engine='openpyxl')

    # Remove the JSON file
    os.remove(json_file)

# Example usage
convert_json_to_excel('output.json', 'output.xlsx')



