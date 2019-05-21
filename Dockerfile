from ubuntu:latest

RUN apt-get update \
    && apt-get install -y wget \
    && wget https://github.com/denoland/deno/releases/download/v0.5.0/deno_linux_x64.gz \
    && gzip -d deno_linux_x64.gz \
    && cp deno_linux_x64 /usr/bin/deno \
    && chmod +x /usr/bin/deno

WORKDIR /app

COPY . .
env DOCKERMODE=1
CMD ["deno","run","-A","test.ts"]