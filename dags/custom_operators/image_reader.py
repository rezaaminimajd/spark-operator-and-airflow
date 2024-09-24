import gitlab

from airflow.models import Variable
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LatestImageOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(LatestImageOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        variables = Variable.get("example", deserialize_json=True)

        GITLAB_API_URL = variables["GITLAB_API_URL"]
        GITLAB_ACCESS_TOKEN = variables["GITLAB_ACCESS_TOKEN"]
        SPARK_IMAGE_NAME = variables["SPARK_IMAGE_NAME"]
        GITLAB_PROJECT_ID = variables["GITLAB_PROJECT_ID"]

        gl = gitlab.Gitlab(GITLAB_API_URL, private_token=GITLAB_ACCESS_TOKEN)
        gl.auth()
        project = gl.projects.get(GITLAB_PROJECT_ID)
        repositories = project.repositories.list()
        for repo in repositories:
            if repo.path == SPARK_IMAGE_NAME:
                tags = repo.tags.list(all=True)
                latest_tag = sorted(tags, key=lambda tag: repo.tags.get(tag.name).created_at)[-1].name
                self.log.info(f"Latest Spark Image: {latest_tag}")
                self.xcom_push(context, value=latest_tag)
