# Imagen única para coordinador, nodos y dashboard (solo biblioteca estándar)
FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1
COPY distritask ./distritask
COPY scripts ./scripts
# Por defecto arranca un nodo trabajador; docker-compose sobreescribe el comando.
CMD ["python", "-m", "distritask.worker", "--bind", "0.0.0.0", "--coordinator", "coordinator:5000"]
