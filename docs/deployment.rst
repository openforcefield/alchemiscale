.. _deployment:

##########
Deployment
##########

``alchemiscale`` consists of multiple services that can be independently scaled.
This document details on how to deploy and configure the "server" services in a number of ways.
The "server" services need not be deployed to the same physical host, though you may choose to do so.

Only Linux is supported as a platform for deploying ``alchemiscale`` services; Windows and OSX are not recommended as deployment targets.


.. _deploy-docker-compose:

******************************************
Single-host deployment with docker-compose
******************************************

An alchemiscale "server" deployment consists of a ``neo4j`` database (the "state store"), a client API endpoint, a compute API endpoint, and a reverse proxy (``traefik``).
The client and compute API endpoints can be scaled by adjusting the number of workers.
A single ``docker-compose.yml`` file defines all of these services.
Because our deployment process is containerized, the only requirement for the host is to be able to run ``docker compose`` in a ``x86_64`` environment.
Installation of ``alchemiscale`` software dependencies is unnecessary on the host itself.

The "server" also requires an object store; see :ref:`deploy-object-store`.

.. _deploy-docker-compose-instructions:

Deployment instructions
=======================

Install `docker compose <https://docs.docker.com/compose/install/#scenario-two-install-the-compose-plugin>`_.
We recommend using "Scenario two: Install the Compose plugin" since Docker Desktop may require a paid subscription.
First install the `docker engine <https://docs.docker.com/engine/install/#server>`_ and then install the `plugin <https://docs.docker.com/compose/install/linux/>`_.

Now clone the repository and then navigate to the ``alchemiscale/docker/alchemiscale-server`` folder::
    
    $ git clone https://github.com/OpenFreeEnergy/alchemiscale.git
    $ cd alchemiscale/docker/alchemiscale-server

.. note ::
   It is not strictly necessary to clone the repository. 
   For host deployment, only ``alchemiscale/docker/alchemiscale-server/docker-compose.yml`` and either a ``.env`` file and/or environment variables set are needed.
   By cloning the repository, a ``git pull`` can be used to retrieve an updated ``.env.template`` and ``docker-compose.yml`` which may be useful.

Now make a copy of ``.env.template``:

.. code-block:: bash
   
   $ cp .env.template .env

and modify ``.env`` with your favorite text editor.

.. warning::
   The ``.env`` file will contain sensitive information and should not be checked into version control or shared publicly.

See ``.env.testing`` for an example. 

The ``neo4j`` database requires the directory for the data store to exist before it starts.
This location should be on a storage medium that can handle the IOPS demand of a ``neo4j`` database.
For example, using the location set in ``.env.testing``::

    $ mkdir -p data/server

Now start the service with::

    $ USER_ID=$(id -u) GROUP_ID=$(id -g) docker compose up -d

We set ``USER_ID`` and ``GROUP_ID`` to be the same as the user running the ``docker compose up -d`` command.


Setting up a host on AWS EC2
============================

.. Note:: This is a guide on how to setup a fresh EC2 x86_64 instance running a Amazon Linux 2023 AMI.
   These steps should generally work for other Linux distributions, but may require some modification e.g. the package manager may be ``apt`` instead of ``dnf``.


Once connected to the instance, run the following commands::

    $ sudo dnf check-release-update  # Check for updates
    $ sudo dnf --releasever=version update  # Update if new version is available NOTE: This guide used Amazon Linux Version 2023.1.20230705
    $ sudo dnf -y install docker git
    $ sudo service docker start  # Start docker service
    $ sudo systemctl enable docker.service  # Start docker service on boot
    $ sudo usermod -a -G docker ec2-user  # Add ec2-user to docker group
    $ newgrp docker  # Trick so we don't have to reboot (or login and logout) after adding ec2-user to docker group
    # Now we have to manually install the docker compose plugin until this issue gets resolved https://github.com/amazonlinux/amazon-linux-2023/issues/186
    $ DOCKER_CONFIG=${DOCKER_CONFIG:-$HOME/.docker}  # Set location to install plugin 
    $ mkdir -p $DOCKER_CONFIG/cli-plugins  # Create the directory to install the plugin
    $ curl -SL https://github.com/docker/compose/releases/download/v2.22.0/docker-compose-linux-x86_64 -o $DOCKER_CONFIG/cli-plugins/docker-compose  # Download plugin
    $ chmod +x $DOCKER_CONFIG/cli-plugins/docker-compose  # Set executable permissions to the plugin 
    $ docker info  # Test if everything works
    $ docker compose version  # Test if plugin was installed correctly

Now the instance has all of the dependencies required for ``docker-compose``-based deployment (:ref:`deploy-docker-compose-instructions`)


.. _deploy-kubernetes:

*************************************************
Kubernetes-based deployment with alchemiscale-k8s
*************************************************

To deploy ``alchemiscale`` to a Kubernetes cluster, review the resources defined and detailed in `alchemiscale-k8s`_.

.. _alchemiscale-k8s: https://github.com/datryllic/alchemiscale-k8s


.. _deploy-object-store:

**************************
Setting up an object store
**************************

An "object store" is also needed for a complete server deployment.
Currently, the only supported object store is AWS S3.

Create a private AWS S3 bucket, then provide the following environment variables to the client and compute API services:

``AWS_S3_BUCKET``
    The name of the AWS S3 bucket to use.

``AWS_S3_PREFIX``
    The prefix within the bucket to use for all objects; typically set to ``objectstore``

``AWS_DEFAULT_REGION``
    The AWS region the bucket exists in.

If your API services are deployed on AWS resources, you should grant those resources `role-based <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html>`_ access to S3.
If your API services are deployed on resources outside AWS, you will need to give your services an access key on a user account with S3 access permissions.

``AWS_ACCESS_KEY_ID``
    The ID of the access key.

``AWS_SECRET_ACCESS_KEY``
    The access key content itself.

No additional setup is required for the object store.
