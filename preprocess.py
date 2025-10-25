import pandas as pd
import re
from datetime import datetime
from collections import defaultdict
from typing import Dict, List, Tuple

import logging

logger = logging.getLogger(__name__)

# Переносим функцию calculate_commit_complexity в preprocess.py
def calculate_commit_complexity(commit: dict) -> float:
    """
    Рассчитывает сложность коммита на основе различных факторов.
    Возвращает коэффициент сложности от 0.5 до 3.0
    """
    complexity = 1.0  # Базовая сложность

    message = commit.get("message", "").lower()
    branches = commit.get("branches", [])

    # Факторы сложности
    factors = {
        # Сложность по типу коммита
        "feature": 1.5,
        "refactor": 1.8,
        "fix": 1.3,
        "test": 1.1,
        "docs": 0.8,
        "other": 1.0
    }

    # Определяем тип для расчета сложности
    commit_type = improved_classify_commit(message)
    complexity *= factors.get(commit_type, 1.0)

    # Бонус за сложные ключевые слова в сообщении
    complex_keywords = [
        "architecture", "refactor", "performance", "optimize",
        "security", "migration", "integration", "database"
    ]
    if any(keyword in message for keyword in complex_keywords):
        complexity *= 1.4

    # Бонус за работу в релизных ветках
    release_branches = ["main", "master", "release", "prod", "stable"]
    if any(branch in str(branches).lower() for branch in release_branches):
        complexity *= 1.3

    # Бонус за коммиты с задачами (скорее всего, это осознанная работа)
    if '#' in message:
        complexity *= 1.2

    return min(complexity, 3.0)  # Ограничиваем максимальную сложность

def improved_classify_commit(message: str) -> str:
    """
    Улучшенная классификация коммитов с использованием регулярных выражений
    для предотвращения ложных срабатываний.
    """
    if not message:
        return "other"

    msg = message.lower().strip()

    # Регулярные выражения для точного определения типа коммита
    patterns = {
        "feature": [
            r'^(feat|feature|add|implement|create|new)(\([^)]+\))?:',
            r'\b(adds?|implements?|creates?|new)\b.*\b(feature|functionality|module)\b'
        ],
        "fix": [
            r'^(fix|bug|error|issue|resolve|patch|repair)(\([^)]+\))?:',
            r'\b(fix|fixes|fixed|bug|error|issue|resolve)\b.*\b(#\d+)?\b'
        ],
        "refactor": [
            r'^(refactor|cleanup|remove|optimize|improve|restructure)(\([^)]+\))?:',
            r'\b(refactor|cleanup|optimize|improve|restructure)\b.*\b(code|performance)\b'
        ],
        "test": [
            r'^(test|spec|coverage|unittest|integration)(\([^)]+\))?:',
            r'\b(test|tests|testing|spec|coverage)\b.*\b(add|implement|create)\b'
        ],
        "docs": [
            r'^(doc|readme|comment|document|changelog)(\([^)]+\))?:',
            r'\b(doc|docs|documentation|readme|changelog|comment)\b'
        ]
    }

    # Проверяем каждый паттерн
    for commit_type, regex_list in patterns.items():
        for pattern in regex_list:
            if re.search(pattern, msg):
                return commit_type

    return "other"


def extract_commits_from_data(data: dict) -> Tuple[List[Dict], Dict[str, List[Dict]]]:
    """
    Универсальное извлечение коммитов из разных форматов данных.
    """
    commits = []
    authors_commits = defaultdict(list)

    # Определяем формат данных и извлекаем коммиты
    if "commits" in data:
        commits = data.get("commits", [])
    elif "project" in data and "repos" in data["project"]:
        for repo in data["project"].get("repos", []):
            commits.extend(repo.get("commits", []))
    elif "repos" in data:
        for repo in data.get("repos", []):
            commits.extend(repo.get("commits", []))

    # Группируем коммиты по авторам
    for commit in commits:
        author_name = commit.get("author", {}).get("name", "Unknown")
        authors_commits[author_name].append(commit)

    return commits, dict(authors_commits)


def parse_date(date_str: str) -> datetime:
    """Универсальный парсер дат"""
    if not date_str:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S"
    ]

    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue

    logger.warning(f"Не удалось распарсить дату: {date_str}")
    return None


def extract_features_from_project(data: dict) -> pd.DataFrame:
    """
    Преобразует JSON из backend API в DataFrame с вычисляемыми признаками.
    """
    all_commits, authors_commits = extract_commits_from_data(data)

    if not all_commits:
        return create_empty_features_df()

    # Инициализация счетчиков
    metrics = initialize_metrics()

    # Обработка всех коммитов
    process_all_commits(all_commits, metrics)

    # Расчет производных метрик
    team_size = len(authors_commits)
    derived_metrics = calculate_derived_metrics(metrics, team_size)

    # Расчет реального bus factor
    bus_factor = calculate_real_bus_factor(authors_commits)

    # Расчет средней сложности коммитов
    avg_complexity = calculate_average_complexity(all_commits)

    return create_features_dataframe(metrics, derived_metrics, bus_factor, team_size, avg_complexity)


def calculate_average_complexity(commits: List[Dict]) -> float:
    """Рассчитывает среднюю сложность коммитов в проекте"""
    if not commits:
        return 1.0

    total_complexity = sum(calculate_commit_complexity(commit) for commit in commits)
    return total_complexity / len(commits)


def initialize_metrics() -> dict:
    """Инициализация метрик"""
    return {
        "commits_total": 0,
        "merge_conflicts": 0,
        "refactor_commits": 0,
        "fix_commits": 0,
        "feature_commits": 0,
        "docs_commits": 0,
        "test_commits": 0,
        "active_days": set()
    }


def process_all_commits(commits: List[Dict], metrics: dict):
    """Обработка всех коммитов для сбора метрик"""
    for commit in commits:
        metrics["commits_total"] += 1

        # Анализ сообщения коммита с улучшенной классификацией
        msg = commit.get("message", "")
        ctype = improved_classify_commit(msg)
        update_commit_type_metrics(ctype, metrics)

        # Анализ merge конфликтов
        if len(commit.get("parents", [])) > 1:
            metrics["merge_conflicts"] += 1

        # Анализ даты
        process_commit_date(commit, metrics)


def update_commit_type_metrics(commit_type: str, metrics: dict):
    """Обновление счетчиков типов коммитов"""
    type_mapping = {
        "refactor": "refactor_commits",
        "fix": "fix_commits",
        "feature": "feature_commits",
        "docs": "docs_commits",
        "test": "test_commits"
    }

    if commit_type in type_mapping:
        metrics[type_mapping[commit_type]] += 1


def process_commit_date(commit: Dict, metrics: dict):
    """Обработка даты коммита"""
    created_at = commit.get("createdAt") or commit.get("created_at")
    if created_at:
        dt = parse_date(created_at)
        if dt:
            metrics["active_days"].add(dt.date())


def calculate_derived_metrics(metrics: dict, team_size: int) -> dict:
    """Расчет производных метрик"""
    commits_total = metrics["commits_total"]
    if commits_total == 0:
        return {f"{key}_ratio": 0 for key in ["refactor", "fix", "feature", "docs", "test"]}

    return {
        "refactor_ratio": metrics["refactor_commits"] / commits_total,
        "fix_ratio": metrics["fix_commits"] / commits_total,
        "feature_ratio": metrics["feature_commits"] / commits_total,
        "docs_ratio": metrics["docs_commits"] / commits_total,
        "test_ratio": metrics["test_commits"] / commits_total,
        "active_days_count": len(metrics["active_days"]),
        "avg_commits_per_dev": commits_total / team_size if team_size > 0 else 0
    }


def create_features_dataframe(base_metrics: dict, derived_metrics: dict, bus_factor: int,
                              team_size: int, avg_complexity: float) -> pd.DataFrame:
    """Создание итогового DataFrame с признаками"""
    features = {
        "commits_total": base_metrics["commits_total"],
        "merge_conflicts": base_metrics["merge_conflicts"],
        "bus_factor": bus_factor,
        "refactor_commits": base_metrics["refactor_commits"],
        "fix_commits": base_metrics["fix_commits"],
        "feature_commits": base_metrics["feature_commits"],
        "docs_commits": base_metrics["docs_commits"],
        "test_commits": base_metrics["test_commits"],
        "refactor_ratio": derived_metrics["refactor_ratio"],
        "fix_ratio": derived_metrics["fix_ratio"],
        "feature_ratio": derived_metrics["feature_ratio"],
        "docs_ratio": derived_metrics["docs_ratio"],
        "test_ratio": derived_metrics["test_ratio"],
        "active_days": derived_metrics["active_days_count"],
        "team_size": team_size,
        "avg_commits_per_dev": derived_metrics["avg_commits_per_dev"],
        "avg_complexity": avg_complexity
    }

    return pd.DataFrame([features])


def create_empty_features_df() -> pd.DataFrame:
    """Создание пустого DataFrame с правильной структурой"""
    empty_features = {
        "commits_total": 0, "merge_conflicts": 0, "bus_factor": 0,
        "refactor_commits": 0, "fix_commits": 0, "feature_commits": 0,
        "docs_commits": 0, "test_commits": 0, "refactor_ratio": 0,
        "fix_ratio": 0, "feature_ratio": 0, "docs_ratio": 0, "test_ratio": 0,
        "active_days": 0, "team_size": 0, "avg_commits_per_dev": 0,
        "avg_complexity": 1.0
    }
    return pd.DataFrame([empty_features])


def calculate_real_bus_factor(developers_commits: dict) -> int:
    """Реальный bus factor - минимальное число разработчиков, делающих 50% работы"""
    if not developers_commits:
        return 0

    commit_counts = sorted([len(commits) for commits in developers_commits.values()], reverse=True)
    total_commits = sum(commit_counts)
    target = total_commits * 0.5

    current_sum = 0
    bus_factor = 0
    for count in commit_counts:
        current_sum += count
        bus_factor += 1
        if current_sum >= target:
            break
    return bus_factor


def extract_individual_metrics(data: dict) -> Dict[str, List[Dict]]:
    """
    Извлекает метрики для каждого разработчика отдельно.
    """
    _, authors_commits = extract_commits_from_data(data)
    return authors_commits


# === Пример локального запуска ===
if __name__ == "__main__":
    import json

    with open("sample_commits.json", "r", encoding="utf-8") as f:
        sample = json.load(f)

    df = extract_features_from_project(sample)
    print("Извлеченные признаки:")
    print(df)
    print(f"\nBus factor: {calculate_real_bus_factor(extract_individual_metrics(sample))}")