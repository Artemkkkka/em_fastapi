**1) Склонируйте проект**
```bash
git clone git@github.com:Artemkkkka/em_fastapi.git
```

**2) Создайте виртуальное окружение и активируйте его**
```bash
python -m venv venv
```
```bash
. venv/Scripts/activate
```

**3) Установите зависимости**
```bash
pip install -r app/scripts/requiremets.txt
```
```bash
pip install -r app/api/requiremets.txt
```

**4) Перейдите в кореньпроека и создайте файл .env**
```bash
cd app
```
```bash
touch .env
```
**5) Соберие и поднимите контейнеры**
```bash
docker-compose up --build
```

**6) Проверьте докуменацию на сайте**
http://127.0.0.1:8000/docs
