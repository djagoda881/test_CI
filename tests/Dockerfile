FROM debian

ENV PATH="/home/viadot/.local/bin:$PATH" \
    DP_REPO_HOME=/home/viadot/postgres 


RUN useradd -ms /bin/bash viadot
USER viadot
COPY postgres/ $DP_REPO_HOME
WORKDIR $DP_REPO_HOME

USER root
RUN apt update
RUN apt upgrade -y
RUN apt install -y python3-pip
RUN chmod a+x setup.sh
USER viadot
RUN echo 'alias ll="ls -alF"' >> ~/.bashrc

RUN ./setup.sh
