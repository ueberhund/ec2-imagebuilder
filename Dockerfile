ARG FUNCTION_DIR="/function"

FROM esridc/arcgis-python:2

ARG FUNCTION_DIR

RUN mkdir -p ${FUNCTION_DIR}
WORKDIR ${FUNCTION_DIR}

RUN pip install --target ${FUNCTION_DIR} boto3 dask[dataframe] awslambdaric

ENV REGION=region
ENV SECRET_NAME=secret
ENV S3_BUCKET_NAME=bucket_name
ENV S3_KEY_NAME=key_name
ENV SSE_KEY_ID=key_id

#Create app.py
COPY index.py ${FUNCTION_DIR}/index.py

# Set runtime interface client as default command for the container runtime
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]

# Pass the name of the function handler as an argument to the runtime
CMD [ "index.handler" ]
