# ベースイメージとして公式のPythonイメージを使用
FROM python:3.10-slim

# 必要なツールをインストール
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Jupyter Labをインストール
RUN pip install --no-cache-dir jupyterlab

# コンテナの作業ディレクトリを設定
WORKDIR /workspace

# Jupyter Labを起動するエントリーポイント
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]

# docker build -t jupyterlab-image .
# docker run -p 8888:8888 -v $(pwd):/workspace jupyterlab-image
