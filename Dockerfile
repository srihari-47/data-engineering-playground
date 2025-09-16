#from pre build pythom3.11 image
FROM python:3.11-slim

#adding maintainers
LABEL maintainers="harisudhan"

#setting working directory name same as repository
WORKDIR /assignment

# Upgrade pip and install uv - there is a chance pip install fails without without upgrade
RUN pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir uv

# Copy uv.lock & pypproject.toml files
COPY pyproject.toml uv.lock ./

#We need to point our uv environment to our python environment, otherwise dependencies will not be installed. So we are creating
#UV_PROJECT_ENVIRONMENT environmental variable
ENV UV_PROJECT_ENVIRONMENT=/usr/local/

#install exact dependencies
RUN uv sync --frozen

# Copy source code
COPY source_code/ ./source_code

# Create non-root user 'isaras' and change the ownership of our assignment directories and its sub directories
RUN useradd isaras  && chown -R isaras:isaras /assignment

#switching to isaras user
USER isaras

#Change the working directory to source_code - for smoother script run
WORKDIR /assignment/source_code
