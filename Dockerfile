FROM ubuntu:16.04

WORKDIR /root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        pkg-config \
        rsync \
        software-properties-common \
        unzip \
        git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN curl -LO http://repo.continuum.io/miniconda/Miniconda-latest-Linux-x86_64.sh \
      && bash Miniconda-latest-Linux-x86_64.sh -p /miniconda -b \
      && rm Miniconda-latest-Linux-x86_64.sh
ENV PATH /miniconda/bin:$PATH

ENV CONDA_ENV_NAME grpc-server
COPY environment.yml  ./environment.yml
RUN conda env create -f environment.yml -n $CONDA_ENV_NAME
ENV PATH /miniconda/envs/${CONDA_ENV_NAME}/bin:$PATH

RUN conda clean -tp -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


RUN python -m grpc_tools.protoc -I proto --python_out=. --grpc_python_out=. proto/market_data.proto

EXPOSE 50051

COPY . /root/
CMD ["python", "grpc_server.py"]