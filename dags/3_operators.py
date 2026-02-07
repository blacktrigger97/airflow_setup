from airflow.sdk import dag, task


@dag(
        dag_id='operators'
)
def first_dag():

    @task.python
    def first_task():
        print("This is first task")

    @task.python
    def second_task():
        print("This is second task")

    @task.bash 
    def run_after_loop():
        return "echo https://airflow.apache.org/"

    # Defining task dependencies
    first = first_task()
    second = second_task()
    run_this = run_after_loop()

    first >> second >> run_this

first_dag()