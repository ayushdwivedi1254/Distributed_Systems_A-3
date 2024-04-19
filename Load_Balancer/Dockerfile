# Dockerfile
FROM python:3.8-slim-bullseye

WORKDIR /app

COPY . /app

RUN pip install --trusted-host pypi.python.org flask requests
RUN pip install sortedcontainers

RUN pip install --upgrade pip --break-system-packages
# RUN pip install --trusted-host pypi.python.org flask --break-system-packages
RUN pip install psycopg2-binary --break-system-packages
# RUN pip install mysql-connector-python --break-system-packages

RUN apt-get update
RUN apt-get -y install sudo

RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"
RUN apt-get update
RUN apt-get -y install docker-ce-cli

ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
  && echo "$USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/$USER \
  && chmod 0440 /etc/sudoers.d/$USER \
  && groupadd docker \
  && usermod -aG docker $USER \
  && chsh -s /bin/zsh $USER
USER $USER

ENV HOME=/home/$USER
# RUN sudo apt-get -y install curl apt-transport-https ca-certificates software-properties-common
# RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
# RUN sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu jammy stable"
# RUN sudo apt-get update
# RUN sudo apt-get -y install docker-ce-cli

# Add Docker's official GPG key:
# RUN sudo apt-get update
# RUN sudo apt-get -y install curl apt-transport-https ca-certificates software-properties-common
# RUN sudo install -m 0755 -d /etc/apt/keyrings
# RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
# RUN sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add the repository to Apt sources:
# RUN echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu jammy stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
# RUN sudo apt-get update
# RUN sudo apt-get install docker-ce

EXPOSE 5000

CMD ["python", "load_balancer.py"]
