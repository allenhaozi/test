# 2022 4paramdigm.com Hackathon
#  LEGO Team
# 
# Default settings applied to all tasks

# [START import_module]
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from pathlib import Path
import jinja2
from slugify import slugify


from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import \
    SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import \
    SparkKubernetesSensor


from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import \
    KubernetesPodOperator
from kubernetes.client import models as k8s

# [END import_module]

def gen_file(jinja2_file, target_path, run_config):
    """ Jinja2 tempalte render function

    Args:
        jinja2_file: jinja2 template file
        target_path: jinja2 rendered result file
        run_config:  template + run_config = executor yaml file
    """
    print(f"got jinja2 template file: {jinja2_file}")
    print(f"got jinja2 template rendered result file: {target_path}")
    print(f"got jinja2 template run_config: {run_config}")
    jinja_file = Path(jinja2_file).resolve()
    
    loader = jinja2.FileSystemLoader(jinja_file.parent)
    jinja_env = jinja2.Environment(autoescape=True,
                                   loader=loader,
                                   lstrip_blocks=True)
    jinja_env.filters["slugify"] = slugify

    template = jinja_env.get_template(jinja_file.name)

    # render template
    template.stream(run_config=run_config).dump(str(target_path))
    print("render template success")



def pre_execute_spark_operator(context):
    cur_path = os.path.abspath(os.path.driname(__file__)) 

    jinja2_file = f"{cur_path}/spark.operator.template.yaml"
    target_path = f"{cur_path}/spark.operator.yaml"
    gen_file(jinja2_file, target_path, context['params'])



# [START kubernetes pod operator]

def get_in_cluster():
    """
    default `in_cluster` is True
    Returns:
        boolean : True
    """
    in_cluster = Variable.get('in_cluster', "True")
    if in_cluster.lower() == "false":
        return False

def pre_execute_kube_pod_operator(context):
    cur_path = Path(__file__)

    jinja2_file = f"{cur_path}/pod.template.yaml"
    target_path = f"{cur_path}/pod.yaml"
    gen_file(jinja2_file, target_path, context['params'])

def gen_k8s_pod_operator(task_name):

    cur_path = Path(__file__)

    o = KubernetesPodOperator(
        task_id=task_name,
        name=task_name,
        do_xcom_push=True,
        is_delete_operator_pod=False,
        log_events_on_failure=True,
        get_logs=True,
        image='',
        image_pull_policy='Always',
        in_cluster=get_in_cluster(),
        pod_template_file=f"{cur_path}/pod.yaml",
        pre_execute=pre_execute_kube_pod_operator,
    )
    return o

# [END kubernetes pod operator]


# [START instantiate_dag]
default_args = {
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "max_active_runs": 1,
}

with DAG(
        "hackathon-lego-team",
        default_args=default_args,
        description="",
        start_date=datetime(2022, 10, 20),
        schedule_interval=None,
        catchup=False,
        dagrun_timeout=timedelta(minutes=600),
) as dag:

    # [START build dag operator]
    tasks = {}


    tasks["split-data"] = SparkKubernetesOperator(
        task_id="split-data",
        namespace="airflow",
        application_file="split-data_tpl.yaml",
        do_xcom_push=True,
        dag=dag,
        pod_template_file=f"{cur_path}/pod.yaml",
        pre_execute=pre_execute_spark_operator,
    )
    # TODO: add sensor tasks["split-data-sensor"] to spark operator
    tasks["split-data-sensor"] = SparkKubernetesSensor(
        task_id="split-data-sensor",
        namespace="airflow",
        attach_log=True,
        application_name=
            "{{ ti.xcom_pull(task_ids='split-data')['metadata']['name'] }}",
        dag=dag,
        poke_interval=60,
        retries=3,
    )





    tasks["make-predictions"] = gen_k8s_pod_operator('make_predictions')



    tasks["report-accuracy"] = SparkKubernetesOperator(
        task_id="report-accuracy",
        namespace="airflow",
        application_file="report-accuracy_tpl.yaml",
        do_xcom_push=True,
        dag=dag,
        pod_template_file=f"{cur_path}/pod.yaml",
        pre_execute=pre_execute_spark_operator,
    )
    # TODO: add sensor tasks["report-accuracy-sensor"] to spark operator
    tasks["report-accuracy-sensor"] = SparkKubernetesSensor(
        task_id="report-accuracy-sensor",
        namespace="airflow",
        attach_log=True,
        application_name=
            "{{ ti.xcom_pull(task_ids='report-accuracy')['metadata']['name'] }}",
        dag=dag,
        poke_interval=60,
        retries=3,
    )



    # [END build dag operator]
    # [START dag orchestration]
    # TODO: double check airflow dag 

    tasks["split-data"] >> tasks["make-predictions"]

    tasks["split-data"] >> tasks["report-accuracy"]

    tasks["make-predictions"] >> tasks["report-accuracy"]

    # [END dag orchestration]
# [END instantiate_dag]