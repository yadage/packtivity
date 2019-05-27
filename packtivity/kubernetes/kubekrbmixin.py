class KubeKrbMixin(object):

    def __init__(self, **kwargs):
        pass

    def inject_kerberos(self, kube_resources):
        for r in kube_resources:
            if r['kind'] == 'Job':
                for c in r['spec']['template']['spec']['initContainers']:
                    self.addKrbContainer(c)
                for c in r['spec']['template']['spec']['containers']:
                    self.addKrbContainer(c)
                r['spec']['template']['spec']['initContainers'].insert(0,
                    self.getKrbInit()
                )
        
                r['spec']['template']['spec']['volumes'].extend([
                    {'emptyDir': {}, 'name': 'kerberos-data'}
                ])
        return kube_resources

    def addKrbContainer(self,data):
        data.setdefault('volumeMounts',[]).extend([
            {'mountPath': '/tmp/kerberos', 'name': 'kerberos-data'}
        ])
        data.setdefault('env',[]).extend([
            {
            'name': 'KRB5CCNAME',
            'value': 'FILE:/tmp/kerberos/tmp_kt'
            },
            {
                'name': 'KRBUSERNAME',
                'valueFrom': {
                    'secretKeyRef': {'key': 'username','name': 'statesecret'}
                }
            },
            {
                'name': 'KRBPASSWORD',
                'valueFrom': {'secretKeyRef': {'key': 'password','name': 'statesecret'}}
            }
        ])

    def getKrbInit(self):
        init = {'command': ['sh',
        '-c',
        'echo $KRBPASSWORD|kinit $KRBUSERNAME@CERN.CH\n'],
        'env': [
            {'name': 'KRB5CCNAME', 'value': 'FILE:/tmp/kerberos/tmp_kt'},
            {'name': 'KRBUSERNAME',
                'valueFrom': {
                    'secretKeyRef': {
                        'key': 'username',
                        'name': 'statesecret'
                    }
                }
            },
            {
                'name': 'KRBPASSWORD',
                'valueFrom': {
                    'secretKeyRef': {
                        'key': 'password',
                        'name': 'statesecret'
                }
            }}
        ],
        'image': 'cern/cc7-base',
        'name': 'makecc',
        'volumeMounts': [
            {'mountPath': '/tmp/kerberos', 'name': 'kerberos-data'}
        ]}
        return init