FROM ubuntu:24.04 AS builder

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

RUN cmake -G Ninja -DCMAKE_BUILD_TYPE=Release . && ninja

FROM ubuntu:24.04 AS runtime

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
    libarrow1600 \
    libparquet1600 \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -u 1000 -m -s /bin/sh runner
USER 1000:1000
WORKDIR /scratch

COPY --from=builder /build/shopping_sessions /usr/local/bin/shopping_sessions

ENTRYPOINT ["/usr/local/bin/shopping_sessions"]
