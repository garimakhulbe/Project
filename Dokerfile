FROM node:0.10-slim
COPY src /src
WORKDIR /src

RUN npm install azure-common log4js azure-arm-resource azure-storage adal-node

ENTRYPOINT ["node","autoscale.js"]
