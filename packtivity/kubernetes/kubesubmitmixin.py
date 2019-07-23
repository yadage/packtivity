from kubernetes import client, config
import logging

log = logging.getLogger(__name__)


class SubmitToKubeMixin(object):
    def __init__(self, **kwargs):
        self.svcaccount = kwargs.get("svcaccount", "default")
        self.namespace = kwargs.get("namespace", "default")
        if kwargs.get("kubeconfig") == "incluster":
            log.info("load incluster config")
            config.load_incluster_config()
        else:
            cfg = kwargs.get("kubeconfig")
            log.info("load config %s", cfg)
            if not cfg:
                config.load_kube_config()
            else:
                config.load_kube_config(cfg)
            import urllib3

            urllib3.disable_warnings()

    def create_kube_resources(self, resources):
        for r in resources:
            if r["kind"] == "Job":
                thejob = client.V1Job(
                    kind=r["kind"],
                    api_version=r["apiVersion"],
                    metadata=r["metadata"],
                    spec=r["spec"],
                )
                client.BatchV1Api().create_namespaced_job(self.namespace, thejob)
                log.info("created job %s", r["metadata"]["name"])
            elif r["kind"] == "ConfigMap":
                cm = client.V1ConfigMap(
                    api_version="v1",
                    kind=r["kind"],
                    metadata={
                        "name": r["metadata"]["name"],
                        "namespace": self.namespace,
                        "labels": self.resource_labels,
                    },
                    data=r["data"],
                )
                client.CoreV1Api().create_namespaced_config_map(self.namespace, cm)
                log.info("created configmap %s", r["metadata"]["name"])

    def get_k8s_job_log(self, jobname):
        pods = client.CoreV1Api().list_namespaced_pod(
            self.namespace, label_selector="job-name={}".format(jobname)
        )
        pods = pods.to_dict()["items"]
        podname = pods[0]["metadata"]["name"]

        try:
            logs = client.CoreV1Api().read_namespaced_pod_log(podname, self.namespace)
        except client.rest.ApiException:
            pass
        return logs

    def delete_created_resources(self, resources):
        for r in resources:
            if r["kind"] == "Job":
                resource_name = r["metadata"]["name"]
                try:
                    client.BatchV1Api().delete_namespaced_job(
                        resource_name, self.namespace
                    )
                except client.rest.ApiException:
                    pass

                try:
                    client.CoreV1Api().delete_collection_namespaced_pod(
                        self.namespace,
                        label_selector="job-name={}".format(resource_name),
                    )
                except client.rest.ApiException:
                    pass

            elif r["kind"] == "ConfigMap":
                resource_name = r["metadata"]["name"]
                try:
                    client.CoreV1Api().delete_namespaced_config_map(
                        resource_name, self.namespace, client.V1DeleteOptions()
                    )
                except client.rest.ApiException:
                    pass

    def submit(self, jobspec):
        proxy_data, kube_resources = self.plan_kube_resources(jobspec)
        self.create_kube_resources(kube_resources)
        return proxy_data

    def ready(self, job_proxy):
        ready = self.determine_readiness(job_proxy)
        if ready and not "ready" in job_proxy:
            log.debug("is first time ready %s", job_proxy["job_id"])
            job_proxy["ready"] = ready
            if job_proxy["last_success"]:
                log.debug("is first success %s delete resources", job_proxy["job_id"])
                self.delete_created_resources(job_proxy["resources"])
        return ready

    def successful(self, job_proxy):
        return job_proxy["last_success"]

    def fail_info(self, resultproxy):
        pass

    def check_k8s_job_status(self, name):
        return client.BatchV1Api().read_namespaced_job(name, self.namespace).status

    def determine_readiness(self, job_proxy):
        ready = job_proxy.get("ready", False)
        if ready:
            return True

        log.debug("actually checking job %s", job_proxy["job_id"])

        job_id = job_proxy["job_id"]
        jobstatus = self.check_k8s_job_status(job_id)
        job_proxy["last_success"] = jobstatus.succeeded
        job_proxy["last_failed"] = jobstatus.failed
        ready = job_proxy["last_success"] or job_proxy["last_failed"]
        if ready:
            log.debug(
                "job %s is ready and successful. success: %s failed: %s",
                job_id,
                job_proxy["last_success"],
                job_proxy["last_failed"],
            )
        return ready
