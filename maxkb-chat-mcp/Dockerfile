# MaxKB 智能体对话 MCP 服务器
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 国内网络默认使用清华镜像源, 构建时可覆盖:
#   docker build --build-arg PIP_INDEX_URL=https://pypi.org/simple -t maxkb-mcp-chat:1.0.0 .
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple

COPY requirements.txt .
RUN pip install --no-cache-dir -i ${PIP_INDEX_URL} -r requirements.txt

COPY server.py .

EXPOSE 8000

CMD ["python", "server.py"]
