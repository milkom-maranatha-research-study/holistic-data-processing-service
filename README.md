# data-processing-service
It's a project that we use to:
- Pull Therapist and Interaction data from Backend
- Perform data processing (extracts, cleaning and transforming)
- Perfrom data aggregation based on input files that are generated from 2nd bullet points
- Sync back post-processed data to the Backend

## Prerequisites
### Docker

All our components run in Docker containers. Development orchestration is handled by _docker-compose_. Therefore, installing Docker on your machine is required. Regarding installation guidelines, please follow the particular links below:

For machines running **MacOS** you can follow steps explained [here](https://docs.docker.com/docker-for-mac/install/)

For machines running **Linux (Ubuntu)** you can follow steps explained [here](https://docs.docker.com/desktop/install/linux-install/)

Please also ensures that _docker-compose_ command is installed.

### How to Start the Project
- Ensures [Backend App](https://github.com/milkom-maranatha-research-study/holistic-backend) is up.
- Ensures [Hadoop Engine](https://github.com/milkom-maranatha-research-study/bde2020-hadoop-base-2.0.0-simplified) is up.
- Copy-paste `.env.example` as `.env`, and then fill in all the credential.
- Run `$ docker network inspect holistic-net`, find the IPv4 address of the `holistic-backend_web_1` container, and then copy it.
- Modifies the `.env` and set the value of `BACKEND_URL` with `http://{ipv4address}:8080`
- Ensures you activate the python virtual environment. See [this](https://docs.python.org/3/library/venv.html#creating-virtual-environments) article on how to make it and activates it.
- Run this command `$ ./run.sh`