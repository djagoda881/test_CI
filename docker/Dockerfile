FROM python:3.10

RUN useradd --create-home -s /bin/bash nesso && \
    chown -R nesso /home/nesso

RUN echo 'alias ll="ls -alF"' >> ~/.bashrc

RUN apt update && apt upgrade -y

ENV PATH="/home/nesso/.local/bin:$PATH"
ENV PIP_DOWNLOAD_CACHE=.pip_download_cache

COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache \
    pip install -r requirements.txt && \
    rm requirements.txt

# Workdir
ENV USER="nesso"
ENV HOME="/home/$USER"

WORKDIR ${HOME}
USER ${USER}
