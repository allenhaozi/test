class ContextHelper(object):
    def __init__(self, metadata, env, pipeline_name):
        self._metadata = metadata
        self._env = env
        self._session = None
        self._pipeline_name = pipeline_name

    @property
    def env(self):
        return self._env

    @property
    def project_name(self):
        return self._metadata.project_name

    @property
    def source_dir(self):
        return self._metadata.source_dir

    @property
    def context(self):
        return self.session.load_context()

    @property
    def pipeline(self):
        return self.context.pipelines.get(self._pipeline_name)

    @property
    def catalog(self):
        return self.context.catalog

    @property
    def pipeline_name(self):
        return self._pipeline_name

    @property
    def session(self):
        from kedro.framework.session import KedroSession

        if self._session is None:
            self._session = KedroSession.create(self._metadata.package_name,
                                                env=self._env)

        return self._session

    @staticmethod
    def init(metadata, env, pipeline_name="__default__"):
        return ContextHelper(metadata, env, pipeline_name)
