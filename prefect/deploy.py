from prefect import flow
from prefect.blocks.system import Secret
from prefect.runner.storage import GitRepository

# Source for the code to deploy (here, a GitHub repo)
SOURCE_REPO="https://github.com/no-one2k/basnya_de.git"

if __name__ == "__main__":

    flow.from_source(
        source=GitRepository(
            url=SOURCE_REPO,
            credentials={
                # We are assuming you have a Secret block named `github-access-token`
                # that contains your GitHub personal access token
                "access_token": Secret.load("github-access-token"),
            },
        ),
        entrypoint="prefect/track_processed_dates.py:track_dates", # Specific flow to run
    ).deploy(
        name="track-dates-deployment",
        parameters={"last_n_days": 3},
        cron="25 8 * * *",  # 12:25 every day dubai time
        work_pool_name="banya-work-pool",
    )