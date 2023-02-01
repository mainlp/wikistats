import pandas as pd
import requests
from pathlib import Path

# https://wikimedia.org/api/rest_v1/#/
# https://wikitech.wikimedia.org/wiki/Analytics/AQS/Wikistats_2


def get_stats(lang, editor_type,
              metric="edited-pages",
              page_type="content", project_type="wikipedia",
              activity_level="all-activity-levels", granularity="monthly",
              start_incl="20220101", end_excl="20230101"):
    url = f"https://wikimedia.org/api/rest_v1/metrics/{metric}/aggregate/"
    url += f"{lang}.{project_type}/{editor_type}/{page_type}/"
    url += f"{activity_level}/{granularity}/{start_incl}/{end_excl}"
    headers = {"Accep": "application/json"}
    r = requests.get(url, headers=headers)
    df = pd.DataFrame.from_dict(r.json()["items"][0]["results"])
    df = df.rename(columns={metric.replace("-", "_"): editor_type})
    return df


def get_stats_all_editors(
        lang,
        metric="edited-pages", page_type="content",
        project_type="wikipedia", activity_level="all-activity-levels",
        granularity="monthly", start_incl="20010101", end_excl="20230101"):
    df = None
    for editor_type in ("user", "anonymous", "group-bot", "name-bot"):
        df_editor = get_stats(lang, editor_type, metric, page_type,
                              project_type, activity_level, granularity,
                              start_incl, end_excl)
        if df is None:
            df = df_editor
        else:
            df = pd.merge(df, df_editor, on='timestamp', how='left')
    df["timestamp"] = df["timestamp"].str[:7]
    return df


def df_summary(df):
    bot_total = df["name-bot"].sum() + df["group-bot"].sum()
    human_total = df["anonymous"].sum() + df["user"].sum()
    human_ratio_total = human_total / (human_total + bot_total)
    df_2022 = df[df.timestamp.str.startswith("2022")]
    bot_2022 = df_2022["name-bot"].sum() + df_2022["group-bot"].sum()
    human_2022 = df_2022["anonymous"].sum() + df_2022["user"].sum()
    human_ratio_2022 = human_2022 / (human_2022 + bot_2022)
    ratio_2022_total = (human_2022 + bot_2022) / (human_total + bot_total)
    return human_ratio_total, human_ratio_2022, ratio_2022_total


if __name__ == "__main__":
    Path("tables").mkdir(parents=True, exist_ok=True)
    metric = "edited-pages"
    lang2stats = {}
    for lang in ("nds", "lb", "fy", "sco", "als", "bar", "frr", "yi", "li",
                 "fo", "vls", "nds-nl", "zea", "stq", "ksh", "pfl", "pdc",
                 "en", "de", "nl", "da", "is"):
        print(lang)
        df = get_stats_all_editors(lang, metric=metric)
        df.to_csv(f"tables/{lang}_wikipedia.tsv", sep="\t")
        stats = df_summary(df)
        lang2stats[lang] = stats

    with open(f"tables/all_{metric}.tsv", "w+", encoding="utf8") as f:
        f.write("LANGUAGE\tBY_HUMANS_ALLTIME\tBY_HUMANS_2022\tRATIO_2022\n")
        for lang in lang2stats:
            stats = lang2stats[lang]
            f.write(f"{lang}\t{stats[0]}\t{stats[1]}\t{stats[2]}\n")
