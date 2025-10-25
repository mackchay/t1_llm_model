from typing import Dict, List
from preprocess import (
    extract_features_from_project,
    extract_individual_metrics,
    calculate_commit_complexity,
    improved_classify_commit,
    parse_date
)

# --- функции KPI расчёта ---
def calculate_individual_kpi(developer_commits: list) -> float:
    if not developer_commits:
        return 0.0
    total = len(developer_commits)
    feature, fix, refactor, test, docs = 0, 0, 0, 0, 0
    total_complexity = 0
    for commit in developer_commits:
        msg = commit.get("message", "")
        ctype = improved_classify_commit(msg)
        total_complexity += calculate_commit_complexity(commit)
        if ctype == "feature": feature += 1
        elif ctype == "fix": fix += 1
        elif ctype == "refactor": refactor += 1
        elif ctype == "test": test += 1
        elif ctype == "docs": docs += 1
    feature_r, fix_r, refactor_r, test_r, docs_r = feature/total, fix/total, refactor/total, test/total, docs/total
    score = (
        feature_r * 40 +
        (1 - fix_r) * 20 +
        refactor_r * 15 +
        test_r * 15 +
        docs_r * 10
    )
    return round(min(max(score, 0), 100), 2)

def calculate_team_kpi(team_data: dict) -> float:
    # простая усреднённая формула
    return round(
        (team_data.get("feature_ratio", 0)*40 +
         (1 - team_data.get("fix_ratio", 0))*20 +
         team_data.get("refactor_ratio", 0)*15 +
         team_data.get("test_ratio", 0)*15 +
         team_data.get("docs_ratio", 0)*10), 2
    )

def calculate_developer_metrics(commits: List[Dict]) -> Dict:
    total = len(commits)
    if total == 0:
        return {
            "feature_ratio": 0, "fix_ratio": 0, "refactor_ratio": 0,
            "test_ratio": 0, "docs_ratio": 0,
            "total_commits": 0, "active_days": 0
        }

    types = {"feature": 0, "fix": 0, "refactor": 0, "test": 0, "docs": 0, "other": 0}
    active_days = set()

    for commit in commits:
        msg = commit.get("message", "")
        ctype = improved_classify_commit(msg)
        if ctype not in types:
            ctype = "other"
        types[ctype] += 1

        dt = parse_date(commit.get("createdAt"))
        if dt:
            active_days.add(dt.date())

    total_typed = sum(types.values()) or 1
    return {
        "feature_ratio": types["feature"] / total_typed,
        "fix_ratio": types["fix"] / total_typed,
        "refactor_ratio": types["refactor"] / total_typed,
        "test_ratio": types["test"] / total_typed,
        "docs_ratio": types["docs"] / total_typed,
        "total_commits": total,
        "active_days": len(active_days)
    }


# --- основная функция для API ---
def prepare_metrics_for_analyzer(data_dict: dict) -> dict:
    """Возвращает JSON для /analyze_kpi"""
    team_features = extract_features_from_project(data_dict)
    team_data = team_features.iloc[0].to_dict()
    team_kpi = calculate_team_kpi(team_data)

    individual_metrics = extract_individual_metrics(data_dict)
    developers = {}
    for author, commits in individual_metrics.items():
        metrics = calculate_developer_metrics(commits)
        kpi = calculate_individual_kpi(commits)
        developers[author] = {"kpi": kpi, "metrics": metrics}

    result = {
        "team_kpi": team_kpi,
        "team_metrics": team_data,
        "developers": developers
    }
    return result
