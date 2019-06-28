# FROM ubuntu:xenial
FROM openjdk:8-jdk
LABEL maintainer="Andrey Antukh <niwi@niwi.nz>"

ARG EXTERNAL_UID=1000

ENV CLOJURE_VERSION=1.10.0.442 \
    LANG=en_US.UTF-8 \
    LC_ALL=C.UTF-8

RUN set -ex; \
    apt-get update && \
    apt-get install -yq \
        locales \
        ca-certificates \
        wget \
        sudo \
        curl \
        bash \
        zsh \
        git \
        rlwrap \
    ; \
    rm -rf /var/lib/apt/lists/*;

RUN set -ex; \
echo "deb http://apt.postgresql.org/pub/repos/apt/ stretch-pgdg main" >> /etc/apt/sources.list; \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -; \
    apt-get update -yq &&  \
    apt-get install -yq  \
        postgresql-9.6 \
        postgresql-contrib-9.6 \
    ;\
    rm -rf /var/lib/apt/lists/*;

COPY pg_hba.conf /etc/postgresql/9.6/main/pg_hba.conf

# Copy user config files
COPY bashrc /root/.bashrc
COPY zshrc /root/.zshrc
COPY vimrc /root/.vimrc
COPY entrypoint.sh /entrypoint.sh
COPY tmux.conf /root/.tmux.conf

RUN set -ex; \
    /etc/init.d/postgresql start \
    && createuser -U postgres -sl devuser \
    && createdb -U devuser test  \
    && /etc/init.d/postgresql stop

RUN set -ex; \
    wget "https://download.clojure.org/install/linux-install-$CLOJURE_VERSION.sh"; \
    chmod +x "linux-install-$CLOJURE_VERSION.sh"; \
    "./linux-install-$CLOJURE_VERSION.sh"; \
    rm -rf "linux-install-$CLOJURE_VERSION.sh"

RUN set -ex; \
    useradd -m -g users -s /bin/zsh -u $EXTERNAL_UID devuser; \
    passwd devuser -d; \
    echo "devuser ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

USER devuser
WORKDIR /home/devuser

# Copy user config files
COPY bashrc /home/devuser/.bashrc
COPY zshrc /home/devuser/.zshrc
COPY vimrc /home/devuser/.vimrc
COPY tmux.conf /home/devuser/.tmux.conf

ENTRYPOINT ["zsh", "/entrypoint.sh"]
