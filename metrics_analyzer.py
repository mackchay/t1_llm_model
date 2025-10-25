import requests
import time
from typing import Dict, Any

class UniversalTeamAnalyzer:
    def __init__(self, model_name: str = "qwen2.5-coder:7b-instruct-q4_K_M"):
        self.model_name = model_name
        self.ollama_url = "http://localhost:11434/api/generate"

    def _send_llm_request(self, prompt: str) -> str:
        try:
            r = requests.post(
                self.ollama_url,
                json={"model": self.model_name, "prompt": prompt, "stream": False},
                timeout=90
            )
            if r.status_code == 200:
                return r.json().get("response", "")
            return f"Ошибка API: {r.status_code}"
        except requests.exceptions.RequestException:
            return "Ошибка: невозможно подключиться к LLM сервису"

    def analyze_team_data(self, data: Dict[str, Any]) -> str:
        """LLM-анализ уже рассчитанных метрик"""
        t = data["team_metrics"]
        kpi = data["team_kpi"]
        devs = data["developers"]

        prompt = f"""Ты — Head of Engineering. Проанализируй команду разработки:

Командный KPI: {kpi}%
Команда: {t.get('team_size', len(devs))} человек
Bus Factor: {t.get('bus_factor', '?')}
Commits total: {t.get('commits_total', '?')}

Распределение типов коммитов:
Фичи: {t.get('feature_ratio', 0)*100:.1f}%
Фиксы: {t.get('fix_ratio', 0)*100:.1f}%
Рефакторинг: {t.get('refactor_ratio', 0)*100:.1f}%
Тесты: {t.get('test_ratio', 0)*100:.1f}%
Документация: {t.get('docs_ratio', 0)*100:.1f}%

Разработчики:
{chr(10).join([f"- {n}: {d['kpi']}%" for n, d in devs.items()])}

Сформулируй:
1. Проблемы команды
2. Сильные стороны
3. Приоритетные действия на 2 недели
4. KPI цели на 2 недели вперед.
"""
        return self._send_llm_request(prompt)

    def smart_emergency_analysis(self, data: Dict[str, Any]) -> str:
        """Анализ без LLM"""
        t = data["team_metrics"]
        kpi = data["team_kpi"]
        devs = data["developers"]

        avg_kpi = sum(d["kpi"] for d in devs.values()) / len(devs) if devs else 0
        analysis = f"АНАЛИЗ КОМАНДЫ (офлайн)\n"
        analysis += "="*50 + "\n"
        analysis += f"Командный KPI: {kpi:.2f}%\nСредний индивидуальный KPI: {avg_kpi:.2f}%\n\n"
        analysis += "Структура работы:\n"
        for m in ["feature_ratio","fix_ratio","refactor_ratio","test_ratio","docs_ratio"]:
            analysis += f" - {m}: {t.get(m,0)*100:.1f}%\n"
        analysis += "\nРазработчики:\n"
        for name, info in devs.items():
            analysis += f" - {name}: {info['kpi']}%\n"
        analysis += "\nРекомендации:\n"
        if t.get("fix_ratio",0) > 0.4:
            analysis += "1. Снизить количество багфиксов.\n"
        if t.get("test_ratio",0) < 0.1:
            analysis += "2. Усилить тестирование.\n"
        if t.get("feature_ratio",0) < 0.3:
            analysis += "3. Увеличить долю фич.\n"
        analysis += "\nАнализ завершен."
        return analysis


def safe_analyze(data: Dict[str, Any]) -> str:
    """Безопасный анализ с fallback"""
    analyzer = UniversalTeamAnalyzer()
    try:
        start = time.time()
        result = analyzer.analyze_team_data(data)
        if not result or result.startswith("Ошибка"):
            print("⚠️ LLM недоступен, используем fallback.")
            result = analyzer.smart_emergency_analysis(data)
        else:
            print(f"✅ LLM-анализ завершен за {time.time()-start:.1f}с.")
        return result
    except Exception as e:
        print(f"❌ Ошибка анализа: {e}")
        return analyzer.smart_emergency_analysis(data)
