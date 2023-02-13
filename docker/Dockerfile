FROM python:3.10

RUN useradd --create-home -s /bin/bash nesso && \
    chown -R nesso /home/nesso

RUN echo 'alias ll="ls -alF"' >> ~/.bashrc

RUN apt update && apt upgrade -y

ENV PIP_DISABLE_PIP_VERSION_CHECK=on
ENV PYTHONUNBUFFERED=1
ENV PATH="/home/nesso/.local/bin:$PATH"

# Workdir
ENV USER="nesso"
ENV HOME="/home/$USER"

WORKDIR ${HOME}
USER ${USER}
