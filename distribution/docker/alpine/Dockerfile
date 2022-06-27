FROM alpine:3 AS builder

RUN apk add --update make npm

# Build UI
COPY quickwit-ui /quickwit/quickwit-ui

WORKDIR /quickwit/quickwit-ui

RUN echo "Building Quickwit UI" \
    && npm install --location=global yarn \
    && make install build

# Build workspace
COPY . /quickwit

WORKDIR /quickwit

RUN echo "Building workspace with feature(s) '$CARGO_FEATURES' and profile '$CARGO_PROFILE'" \
    && cargo build \
        --features $CARGO_FEATURES \
        $(test "$CARGO_PROFILE" = "release" && echo "--release") \
    && echo "Copying binaries to /quickwit/bin" \
    && mkdir -p /quickwit/bin \
    && find target/$CARGO_PROFILE -maxdepth 1 -perm /a+x -type f -exec mv {} /quickwit/bin \;

# Change the default configuration file in order to make the REST,
# gRPC, and gossip services accessible outside of Docker container.
COPY ./config/quickwit.yaml ./config/quickwit.yaml
RUN sed -i 's/#listen_address: 127.0.0.1/listen_address: 0.0.0.0/g' ./config/quickwit.yaml


FROM alpine:3

LABEL org.opencontainers.image.title="Quickwit"
LABEL maintainer="Quickwit, Inc. <hello@quickwit.io>"
LABEL org.opencontainers.image.vendor="Quickwit, Inc."
LABEL org.opencontainers.image.licenses="AGPL-3.0"

WORKDIR /quickwit
RUN mkdir config qwdata
COPY --from=builder /quickwit /usr/local/bin/
COPY --from=builder /config/quickwit.yaml /quickwit/config/quickwit.yaml

ENV QW_CONFIG=/quickwit/config/quickwit.yaml
ENV QW_DATA_DIR=/quickwit/qwdata

ENTRYPOINT ["/usr/local/bin/quickwit"]
