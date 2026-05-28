# Sport Prediction Project
A Python-based software that manages a free sport prediction game on forums.
It runs on Linux (Ubuntu) and can be executed locally or manually/automatically through GitHub Actions.

## Table of Contents
- [Installation](#installation)
- [Dependant tools and accounts](#dependanttoolsaccounts)
- [High level overview and features](#overview)
- [GitHub project repository tree](#githubtree)
- [Current sources](#currentsources)
- [Usage - Entry points](#usage)
- [Error management and impacts](#error)
- [Tests](#tests)
- [Snowflake database architecture](#snowflakearchitecture)
- [Complete Documentation](#documentation)

## Installation<a name="installation"></a>

- Check your Python version
    ```
    python3 --version
    ```  
    If you have multiple versions, you may need to use `python3.10` or `python3.11` explicitly.

- Clone the repository
    ```
    git clone https://github.com/FabienP9/PREDICT_PROJECT.git  
    cd PREDICT_PROJECT
    ```

- Create and activate local environment
    ```
        python3 -m venv predict_env  
        source predict_env/bin/activate 
    ```
- Install the package
    ```
        pip install -e ".[dev]" # if running in dev
        pip install . # if running in prod
    ```

- Install or sign up with [dependant tools and accounts](#dependanttoolsaccounts)

## Dependant tools and accounts<a name="dependanttoolsaccounts"></a>

The software uses the following external tools and accounts:
- **DropBox free account** (https://www.dropbox.com/register) which will contains some input files for a successful run, and some program files results in a stable tree. Full details in [the manual](#documentation)   
   To link the project with DropBox:  
    ```
    curl https://rclone.org/install.sh | sudo bash
    rclone config
    ```  
    → Choose options "New remote"  
    → Give it a name. ex "predict-dropbox"  
    → Choose Dropbox option number   
    → No client_id / no client_secret    
    → Choose "No" to edit advanced config"  
    → Choose yes to authentificate rclone with remote on web browser  
    → Create a file rclone.conf with this format  
    ```
        [dropbox]
        type=dropbox
        token = {xxx}
    ```  
    → Then run this command  
    ```
        base64 rclone.conf
    ```  
    → Copy the result into a GitHub Secret RCLONE_CONFIG_BASE64         

- **Snowflake free account** (https://signup.snowflake.com/), to store predictions and calculations, with at least one user having DML privileges (The software uses its credentials).  
    → The file *snowflake_account_connect.csv* - see [the manual](#documentation) for details - must store the account id  
    → SNOWFLAKE_USERNAME and SNOWFLAKE_PASSWORD are GitHub secrets containing user credentials with DML privileges  

- **DBT free account** (https://www.getdbt.com/signup), to run transformations and calculations in Snowflake using sql files.

- **IMGBB free account** (https://imgbb.com/signup), to push captures of prediction results and rankings online  
    → Once logged in, click About / API / Get API key  
    → Store the key under IMGBB_API_KEY GitHub secrets  

- **Gmail free account and app** (https://accounts.google.com/) to send by email the status of the run, with details of run - only if ran through GitHub Actions  
    → GMAIL_USER is a GitHub secret storing the email adress (sender)  
    → For the program to send email automatically, open google app: myaccount.google.com/apppasswords  
    → Write "predict-project-app" as app name  
    → Store the password given under GMAIL_APP_PASSWORD GitHub secret  
    → Write the list of recipients of emails under RECIPIENT_EMAIL GitHub secrets  

- **Sports league source accounts** for each season in the [scope](#currentsources), a possible account to get games to predict and result:   
    → URL GitHub secrets: *name_given_to_the_source*_URL  
    → Possible credentials GitHub secrets: *name_given_to_the_source*_USERNAME and *name_given_to_the_source*_PASSWORD  

- **Forum accounts** for each forum in the scope, a possible account to read players' predictions, and post prediction template and results  
    → URL GitHub secrets: *name_given_to_the_forum*_URL  
    → Possible credentials GitHub secrets: *name_given_to_the_forum*_USERNAME and *name_given_to_the_forum*_PASSWORD  

## High level overview and features<a name="overview"></a>

```
GitHub Actions schedule (optional - can be run locally) + Sports league schedule
    ↓ 
Check if there is a task to run according to league schedule (with GitHub yaml configuration file)
    ↓ if yes
Get Sources details (Forums, Leagues) (with Python) + DropBox downloads of relevant manual files (with Python)
    ↓
Input data processing (with Python)
    ↓ 
Snowflake ETL pipeline (with Python & DBT)
    ↓ 
Output files generation and post in forum (with Python)
    ↓ 
DropBox upload of new files details (with Python)

```
The workflow:
- Fetch game schedules and results from sport leagues source, and manages program run schedule according to it
- Read predictions from forum topics sources
- Store and compute results in Snowflake, via DBT
- Generate messages locally translated, images, and rankings
- Post predictions templates for players to follow and results back to the forum
- Sync all files through Dropbox for state management, including csv versions of snowflake table for new snowflake account initialization
- Has a full Prod/Test separation environment (on DropBox & SnowFlake)
- Contains Python and DBT tests
- Manages an unique prediction championship logic between teams' fan of the predicted league

## GitHub project repository tree<a name="githubtree"></a>

```
PREDICT_PROJECT/
├── .github/
│   └── workflows/
│       └── # yml files to run the program through GitHub Actions
│
├── code_archive/
│   └── # important obsolete code (Python + dbt) for reference
│
├── database_dbt_management/
│   └── # DBT project folders and files
│       # includes dbt_project.yml and profiles.yml modified during runtime, called by python
|       # also includes yml files for test while running dbt
│
├── file_exemples/
│   └── # files copied from Dropbox as exemple for Section "Files required in DropBox" of the full manual
│
├── src/
│   └── predict_core/
│       ├── # all Python modules, in organized folders
│       │   # includes SQL queries to adress to Snowflake
│   
├── tests/
│   ├── # all Python tests, organized the same way than predict_core
│   │   # each module has one happy path module test and one edgecases module test
│
├── .gitignore
├── manual.md
├── pyproject.toml
├── README.md
├── uv.lock
```

## Current sources<a name="currentsources"></a>

The software currently contains one source of leagues to predict: The French Elite Basketball, called LNB (with LNB_URL for GitHub secrets).  

It currently processes message from one French forum, named BI (with BI_URL, BI_USERNAME, and BI_PASSWORD for GitHub secrets).  

The only language for message posting is French.  

To add more/ change souces, read [the full manual](#documentation).  

## Usage - Entry points<a name="usage"></a>

The system exposes several entry points, to see them all read [the full manual](#documentation).  
The main entry point, which calculates results, posts them on forums, posts predictions templates, or reads messages is called main. 
- Typical local usage:
    ```
        python -m src.predict_core.entry_point.main
    ```
- Typical GitHub Actions usage:  
    → *gitrun_main_auto_prod.yml*  will check if it time to run a scheduled task then run it or do nothing if it is not  
    → *gitrun_main_manual_prod.yml* will run a task manually written (see *output_need_manual_file.csv* on the [full manual](#documentation))  

## Error management and impacts<a name="error"></a>

If an error occurs at any point, the software will behave differently depending on the origin:
- on functions communicating with external tools (Snowflake, ImgBB, DropBox, Forums, sport leagues websites), it will retry 3 times before considering it as a failure. 
- Otherwise it will consider as a failure directly.

The failure stops the program immediately, running essential closing functions, then exit and send a failure email to recipients.  
The decorators handling that behaviour are developped in *src.predict_core.config.config_decorators.py*

If it was a scheduled task, ran automatically through GitHub Actions:
For next GitHub automatic run, the same calendar task will be retried, as it didn't complete.

This ensures that:
- no task is skipped due to an error,  
- no partial state is incorrectly recorded,  
- the system remains consistent across runs.

## Tests<a name="tests"></a>

- Python tests

    The program includes tests for each Python module: one happy paths module, and one edge cases scenarios module.   
    Each test module uses dedicated material files under *tests/materials/*.  
    To run them:
    ```
        pytest # to run all tests
        pytest tests/tests_*/[...]/tests_*/tests.*.py # to run one module test
    ```

- DBT tests

    DBT automatically runs a large number of tests during program execution, to check values on Snowflake database.    
    These tests are defined in the project’s `.yml` files in *database_dbt_management* folder.  
    If tests don't succeed during run, the program stops, and returns a failure to Python.   
    To run them:
    ```
        cd database_dbt_management
        dbt test # to run all tests
        dbt test --select nameofthetest # to run one test
    ```

## Snowflake database architecture<a name="snowflakearchitecture"></a>

The program creates two databases, one for production, one for testing (names can be defined in the file *snowflake_account_connect.csv* - see [the manual](#documentation) for details -)

There are three schemas (= layers):
- landing (managed by Python): Stores raw data from manual files, forum messages, and game extractions, all of them prealably filtered on relevant data

- curated (managed by DBT): Performs DML operations such as updating games/competitions and extracting predictions. 

- consumpted (managed by DBT): Contains calculations and normalized tables:
    - dimensions (seasons, competitions, gameday, games, forum and topic)
    - calculations of scores and points (for each prediction, per user and gameday, per user and season, per team for the prediction championship)

To explore DBT and Snowflake documentation:
```
    cd database_dbt_management
    dbt docs generate
    dbt docs serve
```

## Complete Documentation<a name="documentation"></a>

A full documentation about the game, entry points, DropBox tree,...can be found in the file manual.md

