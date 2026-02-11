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
    && rm -rf /var/lib/apt/lists/*

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

# Copy application code
COPY src/ ./src/
COPY experiment/ ./experiment/

# Expose ports
EXPOSE 22
EXPOSE 8000-8100

# Start SSH and keep container running
CMD ["/usr/sbin/sshd", "-D"]
