FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Установка pip-зависимостей
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Настройка рабочей директории
WORKDIR /workspaces

# Установка дополнительных инструментов (по желанию)
#RUN pip install pylint black mypy

CMD ["/bin/bash"]
