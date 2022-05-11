# Autoretrieve

This directory contains a temporary `autoretrieve` _cluster-level_ Flux tenancy configuration that
creates a K8S namespace dedicated to `autoretrieve`, along with all Flux CRDs to set up and manage
continuous delivery for it.

The rationale is to set up an automated continuous delivery pipeline to facilitate a tight debug
loop while the long term plans are being decided. This deployment also facilitates metrics
forwarding to the PL grafana for `autoretrieve`.

Note that the [_application
level_ manifests](https://github.com/application-research/autoretrieve/tree/main/deploy/manifests/dev/us-east-2)
are located at the `autoretrieve` repo itself.

See:

- https://github.com/application-research/autoretrieve
