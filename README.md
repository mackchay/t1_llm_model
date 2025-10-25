### Генерация датасета и тренировка модели
```
python dataset_generator.py
python train_team_model.py 
python train_individual_model.py 
```

### Сборка
``` 
pip install -r requirements.txt
uvicorn api:app --host 0.0.0.0 --port 8000 --reload
```
