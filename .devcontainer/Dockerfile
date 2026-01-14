FROM python:3.13-slim

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

# Установка расширения ИИ
#RUN mkdir /workspaces/
#COPY .devcontainer/extensions/gigacode-vscode-25.11.671.vsix /tmp/gigacode-vscode-25.11.671.vsix
#RUN code --install-extension /tmp/gigacode-vscode-25.11.671.vsix && rm /tmp/gigacode-vscode-25.11.671.vsix

CMD ["/bin/bash"]
#CMD ["sleep infinity"]