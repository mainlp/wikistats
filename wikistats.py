import requests
import pandas as pd
from pathlib import Path
import sys

# https://wikimedia.org/api/rest_v1/#/
# Edit counts and editor counts are CC0 1.0 per the documentation.


def get_stats(lang, user_agent, editor_type,
              metric="edited-pages",
              page_type="content", project_type="wikipedia",
              activity_level="all-activity-levels", granularity="monthly",
              start_incl="20220101", end_excl="20230101"):
    url = f"https://wikimedia.org/api/rest_v1/metrics/{metric}/aggregate/"
    url += f"{lang}.{project_type}/{editor_type}/{page_type}/"
    if metric != "edits":
        url += activity_level + "/"
    url += f"{granularity}/{start_incl}/{end_excl}"
    headers = {"Accept": "application/json",
               'User-Agent': user_agent,  # per Wikimedia REST API rules
               }
    r = requests.get(url, headers=headers)
    df = pd.DataFrame.from_dict(r.json()["items"][0]["results"])
    df = df.rename(columns={metric.replace("-", "_"):
                            editor_type + "_" + metric})
    return df


def get_stats_all_editors(
        lang, user_agent,
        metric="edits", page_type="content",
        project_type="wikipedia", activity_level="all-activity-levels",
        granularity="monthly", start_incl="20010101", end_excl="20230101"):
    df = None
    for editor_type in ("user", "anonymous", "group-bot", "name-bot",
                        "all-editor-types"):
        df_editor = get_stats(lang, user_agent, editor_type, metric, page_type,
                              project_type, activity_level, granularity,
                              start_incl, end_excl)
        if df is None:
            df = df_editor
        else:
            df = pd.merge(df, df_editor, on='timestamp', how='left')
    df_users = get_stats(lang, user_agent, "user", "editors", page_type,
                         project_type, activity_level, granularity,
                         start_incl, end_excl)
    df = pd.merge(df, df_users, on='timestamp', how='left')
    df["timestamp"] = df["timestamp"].str[:7]
    return df


def df_stats(df, metric):
    m = "_" + metric
    total = df["all-editor-types" + m].sum()
    bot = (df["name-bot" + m].sum() + df["group-bot" + m].sum()) / total
    human = (df["anonymous" + m].sum() + df["user" + m].sum()) / total
    return human, bot, total


def df_summary(df, metric):
    human_total, bot_total, all_total = df_stats(df, metric)
    df_2022 = df[df.timestamp.str.startswith("2022")]
    human_2022, bot_2022, all_2022 = df_stats(df_2022, metric)
    ratio_2022_total = all_2022 / all_total
    monthly_editors_2022 = df_2022["user_editors"].mean()
    return human_total, bot_total, human_2022, bot_2022, ratio_2022_total, monthly_editors_2022


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 wikistats.py USER_AGENT")
        print("(USER_AGENT can be an email address.")
        print("See: https://wikimedia.org/api/rest_v1/ )")
        sys.exit(1)
    user_agent = sys.argv[1]
    Path("tables").mkdir(parents=True, exist_ok=True)
    metric = "edits"
    lang2stats = {}
    for lang in ("nds", "lb", "fy", "sco", "als", "bar", "frr", "yi", "li",
                 "fo", "vls", "nds-nl", "zea", "stq", "ksh", "pfl", "pdc",
                 "en", "de", "nl", "da", "is"):
        print(lang)
        df = get_stats_all_editors(lang, user_agent, metric=metric)
        df.to_csv(f"tables/{lang}_wikipedia.tsv", sep="\t")
        stats = df_summary(df, metric)
        lang2stats[lang] = stats

    with open(f"tables/all_{metric}.tsv", "w+", encoding="utf8") as f:
        f.write("LANGUAGE\tBY_HUMANS_ALLTIME\tBY_BOTS_ALLTIME\tBY_HUMANS_2022\tBY_BOTS_2022\tRATIO_2022\tMONTHLY_EDITORS_2022\n")
        for lang in lang2stats:
            f.write(lang + "\t")
            f.write("\t".join(map(str, lang2stats[lang])))
            f.write("\n")
