import os
import logging
import uuid
import json

from .kubekrbmixin import KubeKrbMixin

log = logging.getLogger(__name__)


class KubeSpecMixin(KubeKrbMixin):
    def __init__(self, **kwargs):
        self.cvmfs_repos = ["atlas.cern.ch", "sft.cern.ch", "atlas-condb.cern.ch"]
        self.secrets = kwargs.get("secrets", {"hepauth": "hepauth", "hepimgcred": []})
        self.authmount = "/recast_auth"
        self.resource_labels = {"component": "yadage"}
        self.resource_labels.update(**kwargs.get("resource_labels", {}))

        self.inject_krb = kwargs.get("inject_krb", False)
        opts = os.environ.get("PACKTIVITY_KUBE_RESOURCE_OPTS")
        self.resource_opts = (
            json.loads(opts)
            if opts
            else kwargs.get(
                "resource_opts",
                os.environ.get(
                    "PACKTIVITY_KUBE_RESOURCE_OPTS",
                    {"requests": {"memory": "2Gi", "cpu": "1"}},
                ),
            )
        )
        self.resource_prefix = kwargs.get("resource_prefix", "wflow")
        KubeKrbMixin.__init__(self, **kwargs)

    def auth_binds(self, authmount=None):
        container_mounts = []
        volumes = []

        authmount = authmount or self.authmount
        log.debug("binding auth")
        volumes.append(
            {
                "name": "hepauth",
                "secret": {
                    "secretName": self.secrets["hepauth"],
                    "items": [
                        {"key": "getkrb.sh", "path": "getkrb.sh", "mode": 493}  # 755
                    ],
                },
            }
        )
        container_mounts.append({"name": "hepauth", "mountPath": authmount})
        return container_mounts, volumes

    def cvmfs_binds(self, repos=None):
        container_mounts = []
        volumes = []
        log.debug("binding CVMFS")
        repos = repos or self.cvmfs_repos
        for repo in repos:
            reponame = repo.replace(".", "").replace("-", "")
            volumes.append(
                {
                    "name": reponame,
                    "persistentVolumeClaim": {"claimName": reponame, "readOnly": True},
                }
            )
            container_mounts.append({"name": reponame, "mountPath": "/cvmfs/" + repo})
        return container_mounts, volumes

    def make_par_mount(self, job_uuid, parmounts):
        parmount_configmap_contmount = []
        configmapspec = {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": "parmount-{}".format(job_uuid)},
            "data": {},
        }

        vols_by_dir_name = {}
        for i, x in enumerate(parmounts):
            configkey = "parmount_{}".format(i)
            configmapspec["data"][configkey] = x["mountcontent"]

            dirname = os.path.dirname(x["mountpath"])
            basename = os.path.basename(x["mountpath"])

            vols_by_dir_name.setdefault(
                dirname,
                {
                    "name": "vol-{}".format(dirname.replace("/", "-")),
                    "configMap": {
                        "name": configmapspec["metadata"]["name"],
                        "items": [],
                    },
                },
            )["configMap"]["items"].append({"key": configkey, "path": basename})

        log.debug(vols_by_dir_name)

        for dirname, volspec in list(vols_by_dir_name.items()):
            parmount_configmap_contmount.append(
                {"name": volspec["name"], "mountPath": dirname}
            )
        return (
            parmount_configmap_contmount,
            list(vols_by_dir_name.values()),
            configmapspec,
        )

    def get_job_mounts(self, job_uuid, jobspec_environment):
        cvmfs = "CVMFS" in jobspec_environment["resources"]
        parmounts = jobspec_environment["par_mounts"]
        auth = "GRIDProxy" in jobspec_environment["resources"]

        kube_resources = []
        container_mounts = []
        volumes = []

        if cvmfs:
            container_mounts_cvmfs, volumes_cvmfs = self.cvmfs_binds()
            container_mounts += container_mounts_cvmfs
            volumes += volumes_cvmfs

        if auth:
            container_mounts_auth, volumes_auth = self.auth_binds()
            container_mounts += container_mounts_auth
            volumes += volumes_auth

        if parmounts:
            container_mounts_pm, volumes_pm, pm_cm_spec = self.make_par_mount(
                job_uuid, parmounts
            )
            container_mounts += container_mounts_pm
            volumes += volumes_pm
            kube_resources.append(pm_cm_spec)
        return kube_resources, container_mounts, volumes

    def container_sequence_fromspec(
        self, sequence_spec, mainmounts=None, configmounts=None
    ):
        configmounts = configmounts or []
        mainmounts = mainmounts or []
        return [
            {
                "name": seqname,
                "image": sequence_spec[seqname]["image"],
                "command": sequence_spec[seqname]["cmd"],
                "env": sequence_spec["config_env"]
                if sequence_spec[seqname]["iscfg"]
                else [],
                "volumeMounts": mainmounts
                + (configmounts if sequence_spec[seqname]["iscfg"] else []),
            }
            for seqname in sequence_spec["sequence"]
        ]

    def get_job_spec_for_sequence(self, jobname, sequence, volumes):
        containers = sequence[-1:]
        initContainers = sequence[:-1]

        return self.get_job_spec(
            jobname,
            initContainers=initContainers,
            containers=containers,
            volumes=volumes,
        )

    def get_cm_spec(self, cmname, data):
        return {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "data": data,
            "metadata": {"name": cmname, "labels": self.resource_labels},
        }

    def get_job_spec(self, jobname, initContainers=None, containers=None, volumes=None):
        containers = containers or []
        initContainers = initContainers or []
        volumes = volumes or []
        for x in containers + initContainers:
            x["resources"] = self.resource_opts
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "securityContext": {"runAsUser": 0},
                        "initContainers": initContainers,
                        "imagePullSecrets": self.secrets["hepimgcred"],
                        "containers": containers,
                        "volumes": volumes,
                    },
                    "metadata": {"name": jobname, "labels": self.resource_labels},
                },
            },
            "metadata": {"name": jobname, "labels": self.resource_labels},
        }
        job["spec"]["template"]["spec"]
        return job

    def plan_kube_resources(self, jobspec):
        job_uuid = str(uuid.uuid4())
        kube_resources, container_mounts, volumes = [], [], []

        container_mounts_state, volumes_state = self.state_mounts_and_vols(jobspec)
        container_mounts += container_mounts_state
        volumes += volumes_state

        resources, mounts, vols = self.get_job_mounts(
            job_uuid, jobspec["spec"]["environment"]
        )
        container_mounts += mounts
        volumes += vols
        kube_resources += resources

        jobname = "{}-job-{}".format(self.resource_prefix, job_uuid)

        config_resources, config_vols, config_mounts = self.config(job_uuid, jobspec)
        kube_resources += config_resources
        volumes = volumes + config_vols

        container_sequence = self.container_sequence_fromspec(
            jobspec["sequence_spec"],
            mainmounts=container_mounts,
            configmounts=config_mounts,
        )

        jobspec = self.get_job_spec_for_sequence(
            jobname, sequence=container_sequence, volumes=volumes
        )
        proxy_data = self.proxy_data(job_uuid, kube_resources)
        kube_resources.append(jobspec)

        if self.inject_krb:
            kube_resources = self.inject_kerberos(kube_resources)
        return proxy_data, kube_resources
