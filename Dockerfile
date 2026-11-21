FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    ninja-build \
    ca-certificates \
    libarrow-dev \
    libparquet-dev \
    nlohmann-json3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY CMakeLists.txt .
COPY src/ src/

RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=Release . && ninja && \
    cp /build/shopping_sessions /usr/local/bin/shopping_sessions && \
    rm -rf /build && \
    useradd -u 1000 -m -s /bin/sh runner

USER 1000:1000
WORKDIR /scratch

ENTRYPOINT ["/usr/local/bin/shopping_sessions"]
