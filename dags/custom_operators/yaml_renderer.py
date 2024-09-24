import pendulum

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from jinja2 import Environment, FileSystemLoader


class RenderYamlOperator(BaseOperator):
    @apply_defaults
    def __init__(self, values, latest_image_task_id, template_path, xcom_key, *args, **kwargs):
        super(RenderYamlOperator, self).__init__(*args, **kwargs)
        self.values = values
        self.template_path = template_path
        self.xcom_key = xcom_key
        self.latest_image_task_id = latest_image_task_id

    def execute(self, context):
        latest_tag = self.xcom_pull(context, task_ids=self.latest_image_task_id)
        self.values['image'] = self.values['image'].format(latest_tag)

        env = Environment(loader=FileSystemLoader(searchpath="path/to/templates"))
        template = env.get_template(self.template_path)
        rendered_yaml = template.render(self.values)
        self.log.info(f"Rendered YAML:\n{rendered_yaml}")
        self.xcom_push(context, self.xcom_key, rendered_yaml)

    @staticmethod
    def __convert_to_tehran_timezone__(context):
        data_interval_start = context.get("data_interval_start")
        data_interval_end = context.get("data_interval_end")

        start_datetime = pendulum.instance(data_interval_start)
        end_datetime = pendulum.instance(data_interval_end)

        start_tehran_time = start_datetime.in_timezone("Asia/Tehran")
        end_tehran_time = end_datetime.in_timezone("Asia/Tehran")

        return start_tehran_time, end_tehran_time
