FROM gcr.io/deeplearning-platform-release/tf2-cpu
COPY . /flights
RUN apt update && \
    apt install --yes python3-pip && \
    pip3 install --upgrade --quiet tf-nightly-2.0-preview cloudml-hypertune

ENV PYTHONPATH ${PYTHONPATH}:/flights
ENTRYPOINT ["python3", "-m", "trainer.task"]
