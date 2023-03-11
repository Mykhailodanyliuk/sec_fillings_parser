FROM python:latest

SHELL ["/bin/bash", "-c"]

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip

RUN apt update && apt -qy install gcc libjpeg-dev libxslt-dev \
    libpq-dev libmariadb-dev libmariadb-dev-compat gettext cron openssh-client \
    flake8 locales vim

RUN useradd -rms /bin/bash user && chmod 777 /opt /run

WORKDIR /sec_parser

COPY --chown=user:user . .

RUN chown -R user:user /sec_parser

RUN pip install -r requirements.txt

USER user

CMD ["python", "sec_fillings_parser.py"]