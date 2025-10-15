FROM python:3.12

ARG VERSION=0.16.0

WORKDIR /app

RUN apt-get update && apt-get install -y curl && \
    curl -L -o toolbox https://storage.googleapis.com/genai-toolbox/v$VERSION/linux/amd64/toolbox && \
    chmod +x toolbox && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

COPY tools.yaml ./

EXPOSE 5000

# CMD ["./toolbox", "--tools-file", "tools.yaml", "--log-level=DEBUG", "--host", "0.0.0.0", "--port", "5000"]
CMD ["./toolbox", "--tools-file", "tools.yaml", "--log-level=DEBUG", "--address=0.0.0.0", "--port=5000", "--ui"]
