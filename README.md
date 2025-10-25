### Сборка
``` 
pip install -r requirements.txt
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```
## Install Ollama
   ```bash
   curl -fsSL https://ollama.ai/install.sh | sh

   ollama pull qwen2.5-coder:7b-instruct-q4_K_M