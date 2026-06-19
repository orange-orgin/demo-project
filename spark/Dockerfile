# 用你已经缓存成功的 tag（不动 bookworm，不碰 docker.io 解析）
FROM python:3.11-slim

# 只保证源指向稳定版（防 trixie），再更新
RUN set -e && \
    if [ -f /etc/apt/sources.list ]; then \
      echo "OK: /etc/apt/sources.list exists"; \
    else \
      # 兼容 slim：把 sources.list.d 里 stable 内容合并出来
      grep -l 'bullseye\|bookworm\|stable' /etc/apt/sources.list.d/*.sources 2>/dev/null || true; \
    fi && \
    apt-get update && apt-get install -y --no-install-recommends \
      openjdk-17-jdk-headless \
      procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN pip install --no-cache-dir "pyspark==3.4.1" py4j

WORKDIR /opt/spark/work
CMD ["python","--version"]