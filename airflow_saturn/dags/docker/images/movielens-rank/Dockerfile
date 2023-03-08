FROM python:3.8-slim

RUN pip install click==8.1.3 pandas==1.5.3

COPY scripts/rank_movies.py /usr/local/bin/rank-movies
RUN chmod +x /usr/local/bin/rank-movies

ENV PATH="/usr/local/bin:${PATH}"