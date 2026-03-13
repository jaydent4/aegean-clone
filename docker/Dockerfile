# Dockerfile for Aegean nodes
FROM golang:1.23

# Fix bad proxy / caching issues for apt
RUN mkdir -p /etc/apt/apt.conf.d && \
    printf '%s\n' \
      'Acquire::http::Pipeline-Depth 0;' \
      'Acquire::http::No-Cache true;' \
      'Acquire::BrokenProxy true;' \
    > /etc/apt/apt.conf.d/99fixbadproxy

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openssh-server \
    ca-certificates \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install oha (prebuilt release binary; supports linux/amd64 + linux/arm64)
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) asset="oha-linux-amd64" ;; \
      arm64) asset="oha-linux-arm64" ;; \
      *) echo "unsupported arch: $arch" >&2; exit 1 ;; \
    esac; \
    wget -O /usr/local/bin/oha "https://github.com/hatoo/oha/releases/latest/download/${asset}"; \
    chmod +x /usr/local/bin/oha; \
    oha --version

# Install k6 (prebuilt release binary, supports linux/amd64 + linux/arm64)
RUN set -eux; \
    arch="$(dpkg --print-architecture)"; \
    case "$arch" in \
      amd64) asset="k6-v1.2.3-linux-amd64.tar.gz" ;; \
      arm64) asset="k6-v1.2.3-linux-arm64.tar.gz" ;; \
      *) echo "unsupported arch: $arch" >&2; exit 1 ;; \
    esac; \
    wget -O /tmp/k6.tar.gz "https://github.com/grafana/k6/releases/download/v1.2.3/${asset}"; \
    tar -xzf /tmp/k6.tar.gz -C /tmp; \
    install -m 0755 "/tmp/${asset%.tar.gz}/k6" /usr/local/bin/k6; \
    rm -rf /tmp/k6.tar.gz "/tmp/${asset%.tar.gz}"; \
    k6 version

# Ensure `go` resolves even in shells with a minimal PATH
RUN ln -sf /usr/local/go/bin/go /usr/bin/go

# Configure SSH for passwordless root login
RUN mkdir -p /run/sshd \
    && mkdir -p /root/.ssh \
    && sed -i 's/^#\?PermitRootLogin .*/PermitRootLogin yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PasswordAuthentication .*/PasswordAuthentication yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?PermitEmptyPasswords .*/PermitEmptyPasswords yes/' /etc/ssh/sshd_config \
    && sed -i 's/^#\?UsePAM .*/UsePAM no/' /etc/ssh/sshd_config \
    && ssh-keygen -A

# Make root password empty and ensure account is unlocked
RUN passwd -d root && passwd -u root || true

# Set working directory
WORKDIR /app

# Expose ports
EXPOSE 22
EXPOSE 8000-8100

# Start SSH and keep container running
CMD ["/usr/sbin/sshd", "-D"]
