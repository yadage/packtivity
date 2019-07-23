import logging

from ..asyncbackends import ExternalAsyncMixin
from ..syncbackends import publish, packconfig, finalize_outputs, finalize_inputs
from ..statecontexts import load_state

from .kubedirectjobbackend import KubernetesDirectJobBackend
from .jobspec import DirectJobMakerMixin

log = logging.getLogger(__name__)


class DirectExternalKubernetesBackend(DirectJobMakerMixin, ExternalAsyncMixin):
    def __init__(self, **kwargs):
        kwargs["job_backend"] = KubernetesDirectJobBackend(**kwargs)
        DirectJobMakerMixin.__init__(self, **kwargs)
        ExternalAsyncMixin.__init__(self, **kwargs)
        self.config = packconfig()

    def result(self, resultproxy):
        state = load_state(resultproxy.statedata, self.deserialization_opts)

        if resultproxy.resultdata is not None:
            return self.datamodel.create(resultproxy.resultdata, state.datamodel)

        parameters = self.datamodel.create(resultproxy.pardata, state.datamodel)

        parameters, state = finalize_inputs(parameters, state)

        pubdata = publish(resultproxy.spec["publisher"], parameters, state, self.config)
        log.info("publishing data: %s", pubdata)
        pubdata = finalize_outputs(pubdata)
        resultproxy.resultdata = pubdata.json()
        return pubdata
